using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;

namespace Utilities
{
    public static class Resolver
    {
        private static Dictionary<Type, Expression> Cache { get; } = new Dictionary<Type, Expression>();
        private static Dictionary<Type, string> KnownTypes = new Dictionary<Type, string>
        {
            [typeof(int)] = $"{nameof(EntityProperty.Int32Value)}",
            [typeof(long)] = $"{nameof(EntityProperty.Int64Value)}",
            [typeof(bool)] = $"{nameof(EntityProperty.BooleanValue)}",
            [typeof(DateTime)] = $"{nameof(EntityProperty.DateTime)}",
            [typeof(DateTimeOffset)] = $"{nameof(EntityProperty.DateTimeOffsetValue)}",
            [typeof(Double)] = $"{nameof(EntityProperty.DoubleValue)}",
            [typeof(Guid)] = $"{nameof(EntityProperty.GuidValue)}",
        };

        private static Dictionary<Type, Expression> Converters = new Dictionary<Type, Expression>();

        // BUGBUG: this will cache the state of the WrapExceptions at the time of generation
        // If a cache is to be used correctly it should include the WrapExceptions state at the time of each call
        // so Dictionary<KeyValuePair<bool, Type>, Expression>
        // But really you should set WrapExceptions once and never change it...I think
        public static bool WrapExceptions { get; set; } = true;
        public static bool TestPropertyExists { get; set; } = true;

        public static EntityResolver<T> From<T>() where T : class, new()
        {
            if (!Cache.TryGetValue(typeof(T), out var rawExpresion))
            {
                rawExpresion = Generator<T>();
                Cache[typeof(T)] = rawExpresion;
            }

            return (rawExpresion as Expression<EntityResolver<T>>).Compile();
        }

        private static Expression Generator<T>() where T : new()
        {
            Expression<EntityResolver<T>> template =
                (string pk, string rk, DateTimeOffset ts, IDictionary<string, EntityProperty> props, string etag) => default(T);

            var rowProps = template.Parameters[3];

            var containsKey = typeof(IDictionary<string, EntityProperty>).GetMethod("ContainsKey");

            var properties = typeof(T).GetProperties().Select(outProperty =>
            {
                var propName = Expression.Constant(outProperty.Name);

                // Use the "Item" accessor to dereference the dictionary here
                var item = Expression.Property(rowProps, "Item", propName);

                // Set the value only if the row has it
                // test = rowProps.ContainsKey(propName) ? GetAccessor(rowProps[propName])()  : default(typeof(T));
                var contains = Expression.Call(rowProps, containsKey, propName);
                var accessor = GetAccessor(item, outProperty.PropertyType);
                var defaultValue = Expression.Default(outProperty.PropertyType);
                var conditionalExpression = Expression.Condition(contains, accessor, defaultValue);
                var accessedValue = TestPropertyExists ? conditionalExpression : accessor;

                // Add a try catch handler that will return a default value if assignment doesn't work
                var catchHandler = Expression.Catch(Expression.Parameter(typeof(System.Exception)), defaultValue);
                var catchBlock = Expression.TryCatch(accessedValue, catchHandler);

                var expression = WrapExceptions ? catchBlock : accessedValue;

                // outProperty = expression,
                return Expression.Bind(outProperty, expression);
            });

            // body = new T { member1 = fromRow, ...};
            var body = Expression.MemberInit(Expression.New(typeof(T)), properties);

            var resolver = Expression.Lambda<EntityResolver<T>>(
               body,
               $"{typeof(T).Name}Resolver",
               false,
               template.Parameters
               );

            return resolver;
        }

        public static void AddConverter<TResult>(Expression<Func<string, TResult>> converter)
        {
            Converters[typeof(TResult)] = converter;
        }

        private static Expression GetAccessor(Expression item, Type type)
        {
            Expression accessor = null;

            var getString = Expression.Property(item, $"{nameof(EntityProperty.StringValue)}");

            if (type == typeof(string))
            {
                return getString;
            }

            if (KnownTypes.TryGetValue(type, out var accessorName))
            {
                accessor = Expression.Coalesce(Expression.Property(item, accessorName), Expression.Default(type));
            }
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var underlying = Nullable.GetUnderlyingType(type);

                if (KnownTypes.TryGetValue(underlying, out accessorName))
                {
                    accessor = Expression.Property(item, accessorName);
                }
            }

            // Do a lookup to see if there is a type that is suitable
            if (accessor == null)
            {
                if (Converters.TryGetValue(type, out var converter))
                {
                    var lambda = converter as LambdaExpression;

                    // Create a temporary variable
                    // this ensures against multiple accesses to the getString in the lambda
                    var variable = Expression.Variable(typeof(string));
                    var assign = Expression.Assign(variable, getString);

                    // inline the lambda by replacing the string parameter with the variable
                    var work = ParameterReplace.Modify(lambda.Body, lambda.Parameters[0], variable);
                    accessor = Expression.Block(variable, assign, work);
                }
            }

            if (accessor == null)
            {
                throw new ResolverException($"GetAccessor didn't recognize the type '{type.Name}' - add a conversion from string");
            }

            return accessor;
        }

        public class ParameterReplace : ExpressionVisitor
        {
            private ParameterExpression Find;
            private Expression Replace;

            static public Expression Modify(Expression expression, ParameterExpression find, Expression replace)
            {
                return new ParameterReplace { Replace = replace, Find = find }.Visit(expression);
            }

            protected override Expression VisitParameter(ParameterExpression p)
            {
                if (p.Name == Find.Name)
                {
                    return Replace;
                }

                return base.VisitParameter(p);
            }
        }

        [Serializable]
        private class ResolverException : System.Exception
        {
            public ResolverException()
            {
            }

            public ResolverException(string message) : base(message)
            {
            }

            public ResolverException(string message, System.Exception innerException) : base(message, innerException)
            {
            }

            protected ResolverException(SerializationInfo info, StreamingContext context) : base(info, context)
            {
            }
        }
    }
}

