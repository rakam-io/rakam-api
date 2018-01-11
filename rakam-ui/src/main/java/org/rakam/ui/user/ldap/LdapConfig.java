package org.rakam.ui.user.ldap;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.concurrent.TimeUnit;

public class LdapConfig {
    private String ldapUrl;
    private String userBindSearchPattern;
    private String groupAuthorizationSearchPattern;
    private String userBaseDistinguishedName;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    @Pattern(regexp = "^ldap(s)?://.*", message = "The URL is invalid. Expected ldaps:// or ldap://")
    public String getLdapUrl() {
        return ldapUrl;
    }

    @Config("ui.authentication.ldap.url")
    @ConfigDescription("URL of the LDAP server")
    public LdapConfig setLdapUrl(String url) {
        this.ldapUrl = url;
        return this;
    }

    @NotNull
    public String getUserBindSearchPattern() {
        return userBindSearchPattern;
    }

    @Config("ui.authentication.ldap.user-bind-pattern")
    @ConfigDescription("Custom user bind pattern. Example: ${USER}@example.com")
    public LdapConfig setUserBindSearchPattern(String userBindSearchPattern) {
        this.userBindSearchPattern = userBindSearchPattern;
        return this;
    }

    public String getGroupAuthorizationSearchPattern() {
        return groupAuthorizationSearchPattern;
    }

    @Config("ui.authentication.ldap.group-auth-pattern")
    @ConfigDescription("Custom group authorization check query. Example: &(objectClass=user)(memberOf=cn=group)(user=username)")
    public LdapConfig setGroupAuthorizationSearchPattern(String groupAuthorizationSearchPattern) {
        this.groupAuthorizationSearchPattern = groupAuthorizationSearchPattern;
        return this;
    }

    public String getUserBaseDistinguishedName() {
        return userBaseDistinguishedName;
    }

    @Config("ui.authentication.ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapConfig setUserBaseDistinguishedName(String userBaseDistinguishedName) {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl() {
        return ldapCacheTtl;
    }

    @Config("ui.authentication.ldap.cache-ttl")
    public LdapConfig setLdapCacheTtl(Duration ldapCacheTtl) {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }
}