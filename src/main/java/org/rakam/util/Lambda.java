package org.rakam.util;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 13:30.
 */
public class Lambda {
    private static String REQUEST_HANDLER_ERROR_MESSAGE = "Request handler method %s.%s couldn't converted to request handler lambda expression %s";
    private static String REQUEST_HANDLER_PRIVATE_ERROR_MESSAGE = "Request handler method %s.%s is not accessible: %s";

    public static <T> T produceLambda(MethodHandles.Lookup lookup, final Method sourceMethod, final Method targetMethod) throws Throwable {
        final MethodHandles.Lookup caller = lookup.in(sourceMethod.getDeclaringClass());
        final MethodHandle implementationMethod = caller.unreflect(sourceMethod);

        final MethodType factoryMethodType = MethodType.methodType(targetMethod.getDeclaringClass());

        final Class<?> methodReturn = targetMethod.getReturnType();
        final Class<?>[] methodParams = targetMethod.getParameterTypes();

        final MethodType functionMethodType = MethodType.methodType(methodReturn, methodParams);

        final CallSite lambdaFactory = LambdaMetafactory.metafactory(
                lookup,
                targetMethod.getName(),
                factoryMethodType,
                functionMethodType,
                implementationMethod,
                implementationMethod.type()
        );

        final MethodHandle factoryInvoker = lambdaFactory.getTarget();

        final T lambda = (T) factoryInvoker.invoke();

        return lambda;
    }
}
