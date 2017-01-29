package org.rakam.ui.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.stripe.exception.APIConnectionException;
import com.stripe.exception.APIException;
import com.stripe.exception.AuthenticationException;
import com.stripe.exception.CardException;
import com.stripe.exception.InvalidRequestException;
import com.stripe.exception.StripeException;
import com.stripe.model.Coupon;
import com.stripe.model.Customer;
import com.stripe.model.Plan;
import com.stripe.net.RequestOptions;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.ProtectEndpoint;
import org.rakam.ui.RakamUIConfig;
import org.rakam.util.RakamException;

import javax.ws.rs.Path;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.ui.user.WebUserHttpService.extractUserFromCookie;

// TODO: CSRF!
@Path("/ui/subscription")
@IgnoreApi
public class UserSubscriptionHttpService
        extends HttpService
{
    private final WebUserService service;
    private final EncryptionConfig encryptionConfig;
    private final RequestOptions requestOptions;

    @Inject
    public UserSubscriptionHttpService(WebUserService service, RakamUIConfig config, EncryptionConfig encryptionConfig)
    {
        this.service = service;
        this.encryptionConfig = encryptionConfig;
        requestOptions = new RequestOptions.RequestOptionsBuilder()
                .setApiKey(config.getStripeKey()).build();
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/plans")
    public List<RakamPlan> listPlans(@CookieParam("session") String session)
    {
        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        Optional<WebUser> webUser = service.getUser(id);
        if (!webUser.isPresent()) {
            throw new RakamException(FORBIDDEN);
        }

        try {
            return Plan.list(ImmutableMap.of("limit", 50), requestOptions).getData().stream()
                    .map(e -> new RakamPlan(e.getId(), e.getName(), e.getAmount(), e.getStatementDescriptor()))
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/create")
    public List<UserSubscription> create(
            @ApiParam("token") String token,
            @ApiParam("coupon") String coupon,
            @ApiParam("plan") String plan,
            @CookieParam("session") String session,
            @HeaderParam("X-Requested-With") String csrfHeader)
    {
        if (!"XMLHttpRequest".equals(csrfHeader)) {
            throw new RakamException(FORBIDDEN);
        }

        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        Optional<WebUser> webUser = service.getUser(id);
        if (!webUser.isPresent() || webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        String userStripeId = service.getUserStripeId(id);

        Customer customer;
        try {
            Map<String, Object> customerParams = new HashMap<>();
            customerParams.put("email", webUser.get().email);
            if (plan != null && !plan.isEmpty()) {
                customerParams.put("plan", plan);
            }
            customerParams.put("source", token);

            if (userStripeId != null) {
                customer = Customer.retrieve(userStripeId, requestOptions);
                if (customer.getDeleted() == Boolean.TRUE) {
                    customer = Customer.create(customerParams, requestOptions);
                }
                else {
                    customer.update(customerParams, requestOptions);
                }
            }
            else {
                customer = Customer.create(customerParams, requestOptions);
            }

            if (plan != null && !plan.isEmpty()) {
                Map<String, Object> subsParams = new HashMap<>();
                subsParams.put("plan", plan);
                if (coupon != null && !coupon.isEmpty()) {
                    subsParams.put("coupon", coupon);
                }
                customer.createSubscription(subsParams, requestOptions);
            }

            service.setStripeId(webUser.get().id, customer.getId());
        }
        catch (InvalidRequestException e) {
            throw new RakamException(e.getMessage(),
                    HttpResponseStatus.valueOf(e.getStatusCode()));
        }
        catch (StripeException e) {
            throw new RakamException(e.getMessage(), BAD_REQUEST);
        }

        return customer.getSubscriptions().getData().stream().map(subs ->
                new UserSubscription(
                        subs.getPlan().getId(),
                        subs.getPlan().getAmount(),
                        Instant.ofEpochMilli(subs.getCurrentPeriodStart()),
                        Instant.ofEpochMilli(subs.getCurrentPeriodEnd()),
                        Optional.ofNullable(subs.getDiscount())
                                .map(e -> e.getCoupon())
                                .map(e -> new RakamCoupon(e.getPercentOff(), e.getAmountOff())).orElse(null)))
                .collect(Collectors.toList());
        // TODO: hasmore?
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/me")
    public List<UserSubscription> me(
            @HeaderParam("X-Requested-With") String csrfHeader,
            @CookieParam("session") String session)
    {
        if (!"XMLHttpRequest".equals(csrfHeader)) {
            throw new RakamException(FORBIDDEN);
        }

        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        Optional<WebUser> webUser = service.getUser(id);
        if (!webUser.isPresent() || webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        String userStripeId = service.getUserStripeId(id);

        if (userStripeId == null) {
            return ImmutableList.of();
        }
        try {
            Customer customer = Customer.retrieve(userStripeId, requestOptions);
            if (customer.getSubscriptions() == null) {
                return ImmutableList.of();
            }
            return customer.getSubscriptions().getData().stream().map(subs ->
                    new UserSubscription(
                            subs.getPlan().getId(),
                            subs.getPlan().getAmount(),
                            Instant.ofEpochSecond(subs.getCurrentPeriodStart()),
                            Instant.ofEpochSecond(subs.getCurrentPeriodEnd()),
                            Optional.ofNullable(subs.getDiscount())
                                    .map(e -> e.getCoupon())
                                    .map(e -> new RakamCoupon(e.getPercentOff(), e.getAmountOff())).orElse(null)))
                    .collect(Collectors.toList());
            // TODO: hasmore
        }
        catch (StripeException e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/coupon")
    public RakamCoupon checkCoupon(
            @ApiParam("coupon") String coupon,
            @HeaderParam("X-Requested-With") String csrfHeader,
            @CookieParam("session") String session)
    {
        if (!"XMLHttpRequest".equals(csrfHeader)) {
            throw new RakamException(FORBIDDEN);
        }

        int id = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        Optional<WebUser> webUser = service.getUser(id);
        if (!webUser.isPresent() || webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        try {
            Coupon retrieve = Coupon.retrieve(coupon, requestOptions);
            return new RakamCoupon(retrieve.getPercentOff(), retrieve.getAmountOff());
        }
        catch (InvalidRequestException e) {
            if (e.getStatusCode() == 404) {
                throw new RakamException(NOT_FOUND);
            }

            throw Throwables.propagate(e);
        }
        catch (StripeException e) {
            throw Throwables.propagate(e);
        }
    }

    public static class RakamCoupon
    {
        public final Integer percentOff;
        public final Integer amountOff;

        @JsonCreator
        public RakamCoupon(@ApiParam("percentOff") Integer percentOff,
                @ApiParam("amountOff") Integer amountOff)
        {
            this.percentOff = percentOff;
            this.amountOff = amountOff;
        }
    }

    public static class RakamPlan
    {
        public final String id;
        public final String name;
        public final Integer amount;
        public final String description;

        @JsonCreator
        public RakamPlan(@ApiParam("id") String id,
                @ApiParam("name") String name,
                @ApiParam("amount") Integer amount,
                @ApiParam("description") String description)
        {
            this.id = id;
            this.name = name;
            this.amount = amount / 100;
            this.description = description;
        }
    }

    public static class UserSubscription
    {
        public final double amount;
        public final Instant currentPeriodStart;
        public final Instant currentPeriodEnd;
        public final RakamCoupon coupon;
        public final String plan;

        @JsonCreator
        public UserSubscription(
                @ApiParam("plan") String plan,
                @ApiParam("amount") double amount,
                @ApiParam("currentPeriodStart") Instant currentPeriodStart,
                @ApiParam("currentPeriodEnd") Instant currentPeriodEnd,
                @ApiParam("coupon") RakamCoupon coupon)
        {
            this.plan = plan;
            this.amount = amount / 100.0;
            this.currentPeriodStart = currentPeriodStart;
            this.currentPeriodEnd = currentPeriodEnd;
            this.coupon = coupon;
        }
    }
}
