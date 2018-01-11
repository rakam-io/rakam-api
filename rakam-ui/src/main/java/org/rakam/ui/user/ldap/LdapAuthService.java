package org.rakam.ui.user.ldap;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.ui.AuthService;
import org.rakam.ui.RakamUIConfig;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import java.security.Principal;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.CharMatcher.JAVA_ISO_CONTROL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.naming.Context.*;

public class LdapAuthService
        implements AuthService {
    private static final Logger log = Logger.get(LdapAuthService.class);

    private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    private final String ldapUrl;
    private final String userBindSearchPattern;
    private final Optional<String> groupAuthorizationSearchPattern;
    private final Optional<String> userBaseDistinguishedName;
    private final Map<String, String> basicEnvironment;
    private final LoadingCache<Credentials, Principal> authenticationCache;
    private final DBI dbi;

    @Inject
    public LdapAuthService(
            LdapConfig serverConfig,
            RakamUIConfig config,
            @Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource) {
        this.dbi = new DBI(dataSource);
        this.ldapUrl = requireNonNull(serverConfig.getLdapUrl(), "ldapUrl is null");
        this.userBindSearchPattern = requireNonNull(serverConfig.getUserBindSearchPattern(), "userBindSearchPattern is null");
        this.groupAuthorizationSearchPattern = Optional.ofNullable(serverConfig.getGroupAuthorizationSearchPattern());
        this.userBaseDistinguishedName = Optional.ofNullable(serverConfig.getUserBaseDistinguishedName());
        if (groupAuthorizationSearchPattern.isPresent()) {
            checkState(userBaseDistinguishedName.isPresent(), "Base distinguished name (DN) for user is null");
        }
        if (config.getHashPassword()) {
            throw new IllegalStateException("You can't enable password hashing if you're using LDAP as authentication management. Please set ui.hash-password=false");
        }

        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY)
                .put(PROVIDER_URL, ldapUrl)
                .build();

        this.basicEnvironment = environment;
        this.authenticationCache = CacheBuilder.newBuilder()
                .expireAfterWrite(serverConfig.getLdapCacheTtl().toMillis(), MILLISECONDS)
                .build(new CacheLoader<Credentials, Principal>() {
                    @Override
                    public Principal load(@Nonnull Credentials key) {
                        return authenticate(key.getUser(), key.getPassword());
                    }
                });
    }

    public static InitialDirContext getInitialDirContext(Map<String, String> environment)
            throws NamingException {
        return new InitialDirContext(new Hashtable<>(environment));
    }

    private static InitialDirContext createDirContext(Map<String, String> environment)
            throws NamingException {
        return getInitialDirContext(environment);
    }

    public boolean login(String username, String password) {
        try {
            Credentials key = new Credentials(username, password);

            authenticationCache.refresh(key);
            authenticationCache.get(key);
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (ExecutionException e) {
            return false;
        }

        return true;
    }

    @Override
    public void checkAccess(int userId) {
        try (Handle handle = dbi.open()) {
            Boolean exists = handle.createQuery("select email, password from web_user where id = :id")
                    .bind("id", userId).map((index, r, ctx) -> {
                        String email = r.getString(1);
                        String password = r.getString(2);
                        try {
                            Principal principal = authenticationCache.get(new Credentials(email, password));
                            if (principal == null) {
                                throw new RakamException("LDAP user doesn't exist", FORBIDDEN);
                            }
                        } catch (UncheckedExecutionException e) {
                            throw Throwables.propagate(e.getCause());
                        } catch (ExecutionException e) {
                            throw new RakamException("LDAP user doesn't exist", FORBIDDEN);
                        }
                        return Boolean.TRUE;
                    }).first();

            if (!Boolean.TRUE.equals(exists)) {
                throw new RakamException("LDAP user doesn't exist in database", FORBIDDEN);
            }
        }
    }

    public Principal authenticate(String user, String password)
            throws RakamException {
        Map<String, String> environment = createEnvironment(user, password);
        InitialDirContext context;
        try {
            context = createDirContext(environment);
            checkForGroupMembership(user, context);

            log.debug("Authentication successful for user %s", user);
            return new LdapPrincipal(user);
        } catch (javax.naming.AuthenticationException e) {
            String formattedAsciiMessage = format("Invalid credentials: %s", JAVA_ISO_CONTROL.removeFrom(e.getMessage()));
            log.debug("Authentication failed for user [%s]. %s", user, e.getMessage());
            throw new RakamException(formattedAsciiMessage, UNAUTHORIZED);
        } catch (NamingException e) {
            log.debug("Authentication failed", e.getMessage());
            throw new RakamException("Authentication failed", INTERNAL_SERVER_ERROR);
        }
    }

    private Map<String, String> createEnvironment(String user, String password) {
        return ImmutableMap.<String, String>builder()
                .putAll(basicEnvironment)
                .put(SECURITY_AUTHENTICATION, "simple")
                .put(SECURITY_PRINCIPAL, createPrincipal(user))
                .put(SECURITY_CREDENTIALS, password)
                .build();
    }

    private String createPrincipal(String user) {
        return replaceUser(userBindSearchPattern, user);
    }

    private String replaceUser(String pattern, String user) {
        return pattern.replaceAll("\\$\\{USER\\}", user);
    }

    private void checkForGroupMembership(String user, DirContext context) {
        if (!groupAuthorizationSearchPattern.isPresent()) {
            return;
        }

        String searchFilter = replaceUser(groupAuthorizationSearchPattern.get(), user);
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        boolean authorized;
        NamingEnumeration<SearchResult> search = null;
        try {
            search = context.search(userBaseDistinguishedName.get(), searchFilter, searchControls);
            authorized = search.hasMoreElements();
        } catch (NamingException e) {
            log.debug("Authentication failed", e.getMessage());
            throw new RakamException("Authentication failed: " + e.getMessage(), INTERNAL_SERVER_ERROR);
        } finally {
            if (search != null) {
                try {
                    search.close();
                } catch (NamingException ignore) {
                }
            }
        }

        if (!authorized) {
            String message = format("Unauthorized user: User %s not a member of the authorized group", user);
            log.debug("Authorization failed for user. " + message);
            throw new RakamException(message, UNAUTHORIZED);
        }
        log.debug("Authorization succeeded for user %s", user);
    }

    private static final class LdapPrincipal
            implements Principal {
        private final String name;

        private LdapPrincipal(String name) {
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LdapPrincipal that = (LdapPrincipal) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class Credentials {
        private final String user;
        private final String password;

        private Credentials(String user, String password) {
            this.user = requireNonNull(user);
            this.password = requireNonNull(password);
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Credentials that = (Credentials) o;

            return Objects.equals(this.user, that.user) &&
                    Objects.equals(this.password, that.password);
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, password);
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("user", user)
                    .add("password", password)
                    .toString();
        }
    }

//    public static void main(String[] args)
//    {
//        LdapConfig ldapConfig = new LdapConfig();
//        ldapConfig.setLdapUrl("ldap://ldap.forumsys.com:389")
//                .setUserBindSearchPattern("cn=read-only-admin,dc=example,dc=com");
//        LdapAuthService ldapAuth = new LdapAuthService(ldapConfig, new RakamUIConfig(), null);
//
//        ldapAuth.authenticate("riemann", "password");
//    }
}