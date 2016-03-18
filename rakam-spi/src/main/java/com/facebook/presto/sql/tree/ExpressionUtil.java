package com.facebook.presto.sql.tree;

public class ExpressionUtil {
    public static <R, C> R accept(Expression expression, AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(expression, context);
    }
}
