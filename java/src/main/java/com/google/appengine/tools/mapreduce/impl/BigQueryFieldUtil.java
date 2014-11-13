package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.BigQueryDataField;
import com.google.appengine.tools.mapreduce.BigQueryFieldMode;
import com.google.appengine.tools.mapreduce.BigQueryIgnore;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for processing {@link Field} and BigQuery data fields.
 */
public final class BigQueryFieldUtil {

  // To ignore the 'this' field present in inner classes.
  private static final String THIS_FIELD_PREFIX = "this$";

  private static final Set<Class<?>> UNSUPPORTED_TYPES =
      new ImmutableSet.Builder<Class<?>>().add(Object.class).build();

  private BigQueryFieldUtil() {
    // utility class
  }

  /**
   * Filters to identify the class {@link Field}s that should not be marshalled to bigquery fields.
   */
  @SuppressWarnings("unchecked")
  private static Predicate<? super Field> ignoreFieldsFilter = Predicates.not(Predicates.or(
      withAnnotation(BigQueryIgnore.class), withModifier(Modifier.TRANSIENT),
      withModifier(Modifier.STATIC), withPrefix(THIS_FIELD_PREFIX)));

  /**
   * @param field - {@link Field} of a class.
   * @return {@link BigQueryDataField} annotated name. If annotation is not present then returns the
   *         name of the field.
   */
  public static String getFieldName(Field field) {
    BigQueryDataField bq = field.getAnnotation(BigQueryDataField.class);
    if (bq != null && !Strings.isNullOrEmpty(bq.name())) {
      return bq.name();
    }
    return field.getName();
  }

  /**
   * @param field - {@link Field} of a class.
   * @return {@link BigQueryDataField} annotation description. Null if annotation is not present.
   */
  public static String getFieldDescription(Field field) {
    BigQueryDataField bq = field.getAnnotation(BigQueryDataField.class);
    if (bq == null || bq.description().equals("")) {
      return null;
    }
    return bq.description();
  }

  /**
   * @param field - {@link Field} of a class.
   * @return {@link BigQueryDataField} annotation mode.
   */
  public static String getFieldMode(Field field) {
    BigQueryDataField bq = field.getAnnotation(BigQueryDataField.class);
    if (bq == null) {
      if (field.getType().isPrimitive()) {
        return BigQueryFieldMode.REQUIRED.getValue();
      }
      return null;
    }
    return bq.mode().getValue();
  }

  /**
   * @param parameterizedType
   * @return runtime type of the generic parameter.
   */
  public static Type getParameterType(ParameterizedType parameterizedType) {
    return parameterizedType.getActualTypeArguments()[0];
  }

  /**
   * @param type
   * @return true if the type can be assigned to a {@link Collection} class
   */
  public static boolean isCollection(Class<?> type) {
    return Collection.class.isAssignableFrom(type);
  }

  /**
   * @param type
   * @return true if the field is a parameterized generic type.
   */
  public static boolean isGenericType(Type type) {
    if (isParameterized(type)) {
      return true;
    }
    // check for case when the type is defined to be parameterized but is used in raw form
    TypeVariable<?>[] typeParameters = ((Class<?>) type).getTypeParameters();
    return typeParameters.length > 0;
  }

  /**
   * @param type
   * @return true if the parameterized type had a parameter declared at compile time
   */
  public static boolean isParameterized(Type type) {
    return type instanceof ParameterizedType;
  }

  /**
   * @param type
   * @return true if it's a collection or an array type.
   */
  public static boolean isCollectionOrArray(Class<?> type) {
    return isCollection(type) || type.isArray();
  }

  /**
   * Returns a set of all the non-transient, non-static fields without the annotation
   * {@link BigQueryIgnore}.
   */
  public static Set<Field> getFieldsToSerialize(Class<?> type) {
    return getAllFields(type, ignoreFieldsFilter);

  }

  /**
   * Bigquery schema cannot be generated in following cases 1. Field is an interface or an abstract
   * type. These types can lead to inconsistent schema at runtime. 2. Generic types. 3. {@link Map}
   * 4. Type having reference to itself as a field.
   */
  static void validateTypeForSchemaMarshalling(Class<?> type) {
    if (isNonCollectionInterface(type) || isAbstract(type)) {
      throw new IllegalArgumentException("Cannot marshal " + type.getSimpleName()
          + ". Interfaces and abstract class cannot be cannot be marshalled into consistent BigQuery data.");
    }
    if (!isCollection(type) && isGenericType(type)) {
      throw new IllegalArgumentException("Cannot marshal " + type.getSimpleName()
          + ". Parameterized type other than Collection<T> cannot be marshalled into consistent BigQuery data.");
    }
    if (Map.class.isAssignableFrom(type)) {
      throw new IllegalArgumentException(
          "Cannot marshal a map into BigQuery data " + type.getSimpleName());
    }
    if (UNSUPPORTED_TYPES.contains(type)) {
      throw new IllegalArgumentException(
          "Type cannot be marshalled into bigquery schema. " + type.getSimpleName());
    }
  }

  private static boolean isNonCollectionInterface(Class<?> type) {
    return !isCollection(type) && type.isInterface();
  }

  private static boolean isAbstract(Class<?> type) {
    // primitive types and Collection interface are abstract. So added this extra check.
    return !type.isPrimitive() && !isCollectionOrArray(type)
        && Modifier.isAbstract(type.getModifiers());
  }

  /**
   * A field of type {@link Collection} must be parameterized for marshalling it into bigquery data
   * as a raw field can lead to ambiguous bigquery table definitions.
   */
  public static void validateCollection(Field field) {
    if (!isParameterized(field.getGenericType())) {
      throw new IllegalArgumentException("Cannot marshal a non-parameterized Collection field "
          + field.getName() + " into BigQuery data");
    }
  }

  /**
   * A field of type Collection<Collection> cannot be marshalled into bigquery data format as
   * parameterized types nested more than one level cannot be determined at runtime. So cannot be
   * marshalled.
   */
  public static void validateNestedRepeatedType(Class<?> parameterType, Field field) {
    if (isCollectionOrArray(parameterType)) {
      throw new IllegalArgumentException(
          " Cannot marshal a nested collection or array field " + field.getName());
    }
  }

  /**
   * Parameterized types nested more than one level cannot be determined at runtime. So cannot be
   * marshalled.
   */
  public static void validateNestedParameterizedType(Type parameterType) {
    if (isParameterized(parameterType)
        || GenericArrayType.class.isAssignableFrom(parameterType.getClass())) {
      throw new IllegalArgumentException(
          "Invalid field. Cannot marshal fields of type Collection<GenericType> or GenericType[].");
    }
  }

  /**
   * Returns type of the parameter or component type for the repeated field depending on whether it
   * is a collection or an array.
   */
  public static Class<?> getParameterTypeOfRepeatedField(Field field) {
    Class<?> componentType = null;
    if (isCollection(field.getType())) {
      validateCollection(field);
      Type parameterType = getParameterType((ParameterizedType) field.getGenericType());
      validateNestedParameterizedType(parameterType);
      componentType = (Class<?>) parameterType;
    } else if (field.getType().isArray()) {
      componentType = field.getType().getComponentType();
      return componentType;
    } else {
      throw new IllegalArgumentException("Unsupported repeated type " + field.getType()
          + " Allowed repeated fields are Collection<T> and arrays");
    }
    validateNestedRepeatedType(componentType, field);
    return componentType;
  }

  static Object getFieldValue(Field field, Object object) {
    try {
      field.setAccessible(true);
      return field.get(object);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Failed to read the value of field " + field.getName(), e);
    }
  }

  /**
   * Returns true if the field is annotated as a required bigquery field.
   */
  static boolean isFieldRequired(Field field) {
    BigQueryDataField bqAnnotation = field.getAnnotation(BigQueryDataField.class);

    return (bqAnnotation != null && bqAnnotation.mode().equals(BigQueryFieldMode.REQUIRED))
        || field.getType().isPrimitive();
  }

  /**
   * Returns if a {@link BigqueryFieldMarshaller} exists for the field either in the map provided or
   * the internally maintained list.
   */
  static BigqueryFieldMarshaller findMarshaller(Field field,
      Map<Field, BigqueryFieldMarshaller> marshallers) {
    BigqueryFieldMarshaller marshaller = null;
    if (marshallers != null) {
      marshaller = marshallers.get(field);
    }
    if (marshaller == null) {
      marshaller = BigqueryFieldMarshallers.getMarshaller(field.getType());
    }
    return marshaller;
  }

  private static Set<Field> getAllFields(final Class<?> type, Predicate<? super Field> predicate) {
    Set<Field> result = new HashSet<>();
    for (Class<?> t : getAllSuperTypes(type))
      Collections.addAll(result, t.getDeclaredFields());

    return ImmutableSet.copyOf(Collections2.filter(result, predicate));
  }

  private static Set<Class<?>> getAllSuperTypes(final Class<?> type) {
    Set<Class<?>> result = Sets.newHashSet();
    if (type != null) {
      result.add(type);
      result.addAll(getAllSuperTypes(type.getSuperclass()));
      for (Class<?> inter : type.getInterfaces()) {
        result.addAll(getAllSuperTypes(inter));
      }
    }
    return result;
  }

  private static <T extends AnnotatedElement> Predicate<T> withAnnotation(
      final Class<? extends Annotation> annotation) {
    return new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        return input != null && input.isAnnotationPresent(annotation);
      }
    };
  }

  private static <T extends Member> Predicate<T> withModifier(final int mod) {
    return new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        return input != null && (input.getModifiers() & mod) != 0;
      }
    };
  }

  private static <T extends Member> Predicate<T> withPrefix(final String prefix) {
    return new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        return input != null && input.getName().startsWith(prefix);
      }
    };
  }
}
