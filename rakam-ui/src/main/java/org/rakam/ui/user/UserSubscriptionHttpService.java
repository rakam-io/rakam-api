package org.rakam.ui.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.stripe.exception.InvalidRequestException;
import com.stripe.exception.StripeException;
import com.stripe.model.Coupon;
import com.stripe.model.Customer;
import com.stripe.model.Plan;
import com.stripe.net.RequestOptions;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.ProtectEndpoint;
import org.rakam.ui.RakamUIConfig;
import org.rakam.ui.UIPermissionParameterProvider;
import org.rakam.util.RakamException;

import javax.ws.rs.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

// TODO: CSRF!
@Path("/ui/subscription")
@IgnoreApi
public class UserSubscriptionHttpService
        extends HttpService {
    private final WebUserService service;
    private final RequestOptions requestOptions;

    @Inject
    public UserSubscriptionHttpService(WebUserService service, RakamUIConfig config) {
        this.service = service;
        requestOptions = new RequestOptions.RequestOptionsBuilder()
                .setApiKey(config.getStripeKey()).build();
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/plans")
    public List<RakamPlan> listPlans(@javax.inject.Named("user_id") UIPermissionParameterProvider.Project project) {
        Optional<WebUser> webUser = service.getUser(project.userId);
        if (!webUser.isPresent()) {
            throw new RakamException(FORBIDDEN);
        }

        try {
            return Plan.list(ImmutableMap.of("limit", 50), requestOptions).getData().stream()
                    .map(e -> new RakamPlan(e.getId(), e.getName(), e.getAmount(), e.getStatementDescriptor()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/create")
    public List<UserSubscription> create(
            @ApiParam("token") String token,
            @ApiParam(value = "coupon", required = false) String coupon,
            @ApiParam("plan") String plan,
            @javax.inject.Named("user_id") UIPermissionParameterProvider.Project project,
            @HeaderParam("X-Requested-With") String csrfHeader) {
        if (!"XMLHttpRequest".equals(csrfHeader)) {
            throw new RakamException(FORBIDDEN);
        }

        Optional<WebUser> webUser = service.getUser(project.userId);
        if (!webUser.isPresent() || webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        String userStripeId = service.getUserStripeId(project.userId);

        Customer customer;
        try {
            Map<String, Object> customerParams = new HashMap<>();
            customerParams.put("email", webUser.get().email);
            customerParams.put("source", token);

            if (userStripeId != null) {
                customer = Customer.retrieve(userStripeId, requestOptions);
                if (customer.getDeleted() == Boolean.TRUE) {
                    customer = Customer.create(customerParams, requestOptions);
                    userStripeId = customer.getId();
                    service.setStripeId(webUser.get().id, userStripeId);
                } else {
                    customer.update(customerParams, requestOptions);
                }
            } else {
                customer = Customer.create(customerParams, requestOptions);
                userStripeId = customer.getId();
                service.setStripeId(webUser.get().id, userStripeId);
            }

            if (plan != null && !plan.isEmpty()) {
                Map<String, Object> subsParams = new HashMap<>();
                subsParams.put("plan", plan);
                if (coupon != null && !coupon.isEmpty()) {
                    subsParams.put("coupon", coupon);
                }
                customer.createSubscription(subsParams, requestOptions);
            }

            return Customer.retrieve(userStripeId, requestOptions).getSubscriptions().getData().stream().map(subs ->
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
        } catch (InvalidRequestException e) {
            throw new RakamException(e.getMessage(),
                    HttpResponseStatus.valueOf(e.getStatusCode()));
        } catch (StripeException e) {
            throw new RakamException(e.getMessage(), BAD_REQUEST);
        }
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/me")
    public List<UserSubscription> me(
            @HeaderParam("X-Requested-With") String csrfHeader,
            @javax.inject.Named("user_id") UIPermissionParameterProvider.Project project) {
        if (!"XMLHttpRequest".equals(csrfHeader)) {
            throw new RakamException(FORBIDDEN);
        }

        Optional<WebUser> webUser = service.getUser(project.userId);
        if (!webUser.isPresent() || webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        String userStripeId = service.getUserStripeId(project.userId);

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
        } catch (StripeException e) {
            throw Throwables.propagate(e);
        }
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/coupon")
    public RakamCoupon checkCoupon(
            @ApiParam("coupon") String coupon,
            @HeaderParam("X-Requested-With") String csrfHeader,
            @javax.inject.Named("user_id") UIPermissionParameterProvider.Project project) {
        if (!"XMLHttpRequest".equals(csrfHeader)) {
            throw new RakamException(FORBIDDEN);
        }

        Optional<WebUser> webUser = service.getUser(project.userId);
        if (!webUser.isPresent() || webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        try {
            Coupon retrieve = Coupon.retrieve(coupon, requestOptions);
            return new RakamCoupon(retrieve.getPercentOff(), retrieve.getAmountOff());
        } catch (InvalidRequestException e) {
            if (e.getStatusCode() == 404) {
                throw new RakamException(NOT_FOUND);
            }

            throw Throwables.propagate(e);
        } catch (StripeException e) {
            throw Throwables.propagate(e);
        }
    }

    public static class RakamCoupon {
        public final Integer percentOff;
        public final Integer amountOff;

        @JsonCreator
        public RakamCoupon(@ApiParam("percentOff") Integer percentOff,
                           @ApiParam("amountOff") Integer amountOff) {
            this.percentOff = percentOff;
            this.amountOff = amountOff;
        }
    }

    public static class RakamPlan {
        public final String id;
        public final String name;
        public final Integer amount;
        public final String description;

        @JsonCreator
        public RakamPlan(@ApiParam("id") String id,
                         @ApiParam("name") String name,
                         @ApiParam("amount") Integer amount,
                         @ApiParam("description") String description) {
            this.id = id;
            this.name = name;
            this.amount = amount / 100;
            this.description = description;
        }
    }

    public static class UserSubscription {
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
                @ApiParam("coupon") RakamCoupon coupon) {
            this.plan = plan;
            this.amount = amount / 100.0;
            this.currentPeriodStart = currentPeriodStart;
            this.currentPeriodEnd = currentPeriodEnd;
            this.coupon = coupon;
        }
    }
}
