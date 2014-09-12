package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.Entity.KEY_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.Entity.SCATTER_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import static com.google.appengine.api.datastore.Query.CompositeFilterOperator.AND;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN_OR_EQUAL;
import static com.google.appengine.api.datastore.Query.FilterOperator.LESS_THAN;
import static com.google.appengine.api.datastore.Query.FilterOperator.LESS_THAN_OR_EQUAL;
import static com.google.appengine.api.datastore.Query.SortDirection.ASCENDING;
import static com.google.appengine.api.datastore.Query.SortDirection.DESCENDING;
import static com.google.appengine.tools.mapreduce.inputs.BaseDatastoreInput.createQuery;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilter;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Rating;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Splits an arbitrary datastore query. This is used by {@link DatastoreInput} to shard queries.
 * This is done in one of two ways:
 *
 *  1. If the query contains an inequality filter, the lower and upper bounds are determined (this
 * may involve a querying the datastore) then the range is split naively. This works well when the
 * property that is being queried on is uniformly distributed.
 *
 *  2. If the query does not contain in inequality filter. The query will be partitioned by the
 * entity key. This is done by using the "__scatter__" property to get a random sample of the
 * keyspace and and partitioning based on that. This can result in a poor distribution if there are
 * equality filters on the query that bias the selection with respect to certain regions of
 * keyspace.
 *
 *  The following clauses are not supported by this class: An inequality filter an unsupported type.
 * (Only numeric and date types are currently supported: {@link "https://developers.google.com/appengine/docs/java/datastore/entities#Java_Properties_and_value_types"}
 * )
 *
 *  Filters that are incompatable with datastore cursors such as: Combining multiple clauses with an
 * OR. A filter on a value being NOT_EQUAL. A filter on a value being IN a set. {@link "https://developers.google.com/appengine/docs/java/datastore/queries#Java_Limitations_of_cursors"}
 */
public class DatastoreShardStrategy {

  private static class Range {
    private FilterPredicate lowerBound;
    private FilterPredicate upperBound;

    Range() {}

    Range(FilterPredicate lowerBound, FilterPredicate upperBound) {
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      checkPropertyNameMatches();
    }

    private void checkPropertyNameMatches() {
      if (lowerBound != null && upperBound != null
          && !lowerBound.getPropertyName().equals(upperBound.getPropertyName())) {
        throw new IllegalArgumentException("A range must be defined on only one property found: "
            + lowerBound + " and " + upperBound);
      }
    }

    void setLowerBound(FilterPredicate lowerBound) {
      if (this.lowerBound != null) {
        throw new UnsupportedOperationException(
            "Found both: " + this.lowerBound + " and " + lowerBound + " on the same query.");
      }
      this.lowerBound = lowerBound;
      checkPropertyNameMatches();
    }

    void setUpperBound(FilterPredicate upperBound) {
      if (this.upperBound != null) {
        throw new UnsupportedOperationException(
            "Found both: " + this.upperBound + " and " + upperBound + " on the same query.");
      }
      this.upperBound = upperBound;
      checkPropertyNameMatches();
    }

    String getPropertyName() {
      if (lowerBound != null) {
        return lowerBound.getPropertyName();
      }
      if (upperBound != null) {
        return upperBound.getPropertyName();
      }
      return null;
    }

    FilterPredicate getLowerBound() {
      return lowerBound;
    }

    FilterPredicate getUpperBound() {
      return upperBound;
    }
  }

  private interface Splitter<T extends Serializable & Comparable<T>> {
    SortedSet<T> getSplitPoints(String namespace, String kind, Range range, int numSplits);
  }

  private static class DoubleSplitter implements Splitter<Double> {
    @Override
    public SortedSet<Double> getSplitPoints(String namespace, String kind, Range range,
        int numSplits) {
      return splitRange(((Number) range.getLowerBound().getValue()).doubleValue(),
          ((Number) range.getUpperBound().getValue()).doubleValue(), numSplits);
    }
  }

  private static class LongSplitter implements Splitter<Long> {
    @Override
    public SortedSet<Long> getSplitPoints(String namespace, String kind, Range range,
        int numSplits) {
      return splitRange(((Number) range.getLowerBound().getValue()).longValue(),
          ((Number) range.getUpperBound().getValue()).longValue(), numSplits);
    }
  }

  private static class DateSplitter implements Splitter<Long> {
    @Override
    public SortedSet<Long> getSplitPoints(String namespace, String kind, Range range,
        int numSplits) {
      return splitRange(((Date) range.getLowerBound().getValue()).getTime(),
          ((Date) range.getUpperBound().getValue()).getTime(), numSplits);
    }
  }

  private static class RatingSplitter implements Splitter<Long> {
    @Override
    public SortedSet<Long> getSplitPoints(String namespace, String kind, Range range,
        int numSplits) {
      return splitRange(((Rating) range.getLowerBound().getValue()).getRating(),
          ((Rating) range.getUpperBound().getValue()).getRating(), numSplits);
    }
  }
  /**
   * Uses the scatter property to distribute ranges to segments.
   */
  class ScatterDatastoreQuerySplitter implements Splitter<Key> {

    @Override
    public SortedSet<Key> getSplitPoints(String namespace, String kind, Range range,
        int numSegments) {
      // A scatter property is added to 1 out of every X entities (X is currently 512), see:
      // http://code.google.com/p/appengine-mapreduce/wiki/ScatterPropertyImplementation
      //
      // We need to determine #segments - 1 split points to divide entity space into equal segments.
      // We oversample the entities with scatter properties to get a better approximation.
      // Note: there is a region of entities before and after each scatter entity:
      // |---*------*------*------*------*------*------*---| * = scatter entity, - = entity
      // so if each scatter entity represents the region following it, there is an extra region
      // before the first scatter entity. Thus we query for one less than the desired number of
      // regions to account for the this extra region before the first scatter entity
      int desiredNumScatterEntities = (numSegments) - 1;
      Query scatter = createQuery(namespace, kind).addSort(SCATTER_RESERVED_PROPERTY).setKeysOnly();
      TreeSet<Key> result = new TreeSet<>();
      result.add((Key) range.getLowerBound().getValue());
      for (Entity e : datastore.prepare(scatter).asIterable(withLimit(desiredNumScatterEntities))) {
        result.add(e.getKey());
      }
      result.add((Key) range.getUpperBound().getValue());
      logger.info("Requested " + desiredNumScatterEntities + " scatter entities, retrieved "
          + result.size());
      return result;
    }
  }

  private static final Logger logger = Logger.getLogger(DatastoreShardStrategy.class.getName());

  private final DatastoreService datastore;
  private final Map<Class<?>, Splitter<?>> typeMap;



  DatastoreShardStrategy(DatastoreService datastoreService) {
    typeMap = ImmutableMap.<Class<?>, Splitter<?>>builder()
    // Doubles
        .put(Float.class, new DoubleSplitter()).put(Double.class, new DoubleSplitter())
        // Integers
        .put(Byte.class, new LongSplitter())
        .put(Short.class, new LongSplitter())
        .put(Integer.class, new LongSplitter())
        .put(Long.class, new LongSplitter())
        .put(Date.class, new DateSplitter())
        .put(Rating.class, new RatingSplitter())
        .put(Key.class, new ScatterDatastoreQuerySplitter())
        .build();
    this.datastore = datastoreService;
  }

  /**
   * @return A list of query objects that together cover the same input data as inputQuery.
   * @param query the query to divide into multiple queries
   * @param numSegments the number of queries to divide the inputQuery into. The actual result may
   *        contain fewer if it cannot be divided that finely.
   */
  @SuppressWarnings("deprecation")
  public List<Query> splitQuery(Query query, int numSegments) {
    if (query.getFilterPredicates() != null && !query.getFilterPredicates().isEmpty()) {
      throw new IllegalArgumentException(
          "FilterPredicates are not supported call Query.setFiler() instead.");
    }
    List<Filter> equalityFilters = new ArrayList<>();
    Range range = new Range();
    filterToEqualityListAndRange(query.getFilter(), equalityFilters, range);

    String property = range.getPropertyName();
    if (range.getUpperBound() == null) {
      range.setUpperBound(findFirstPredicate(query.getNamespace(), query.getKind(), equalityFilters,
          property, DESCENDING));
    }
    if (range.getLowerBound() == null) {
      range.setLowerBound(findFirstPredicate(query.getNamespace(), query.getKind(), equalityFilters,
          property, ASCENDING));
    }
    // If no entities exist just pass use the original query.
    if (range.getUpperBound() == null || range.getLowerBound() == null) {
      return ImmutableList.of(query);
    }

    List<Range> ranges = boundriesToRanges(range, getSplitter(range).getSplitPoints(
        query.getNamespace(), query.getKind(), range, numSegments));
    return toQueries(query, equalityFilters, ranges);
  }

  private List<Query> toQueries(Query query, List<Filter> equalityFilters, List<Range> split) {
    List<Query> result = new ArrayList<>(split.size());
    for (Range r : split) {
      Query subQuery = SerializationUtil.clone(query);
      ImmutableList<Filter> f = ImmutableList
          .<Filter>builder()
          .addAll(equalityFilters)
          .add(r.getLowerBound())
          .add(r.getUpperBound())
          .build();
      subQuery.setFilter(new CompositeFilter(AND, f));
      result.add(subQuery);
    }
    return result;
  }

  private void filterToEqualityListAndRange(Filter filter, List<Filter> equality, Range range) {
    if (filter == null) {
      return;
    }
    if (filter instanceof FilterPredicate) {
      FilterPredicate predicate = (FilterPredicate) filter;
      switch (predicate.getOperator()) {
        case EQUAL:
          equality.add(predicate);
          break;
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
          range.setLowerBound(predicate);
          break;
        case IN:
          throw new UnsupportedOperationException(
              "Queries using an IN filter are unsupported because they do not work with cursors.");
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          range.setUpperBound(predicate);
          break;
        case NOT_EQUAL:
          throw new UnsupportedOperationException("Queries using an NOT_EQUAL filter are"
              + " unsupported because they do not work with cursors.");
        default:
          throw new UnsupportedOperationException("Unsupported query FilterPredicate");
      }
    } else if (filter instanceof CompositeFilter) {
      CompositeFilter composite = (CompositeFilter) filter;
      if (CompositeFilterOperator.AND != composite.getOperator()) {
        throw new UnsupportedOperationException(
            "AND is the only supported CompositeFilterOperator for datastore queries.");
      }
      for (Filter f : composite.getSubFilters()) {
        filterToEqualityListAndRange(f, equality, range);
      }
    } else {
      throw new UnsupportedOperationException(
          "Only FilterPredicate or CompositeFilter are supported for datastore queries.");
    }
  }


  private Splitter<?> getSplitter(Range range) {
    Object value = range.getLowerBound().getValue();
    Object type = value == null ? null : value.getClass();
    Splitter<?> splitter = typeMap.get(type);
    if (splitter == null) {
      throw new IllegalArgumentException("Unsupported value type for inequality filter: " + type);
    }
    return splitter;
  }

  private static <T> List<Range> boundriesToRanges(Range orig, SortedSet<T> boundrySet) {
    List<Range> result = new ArrayList<>();
    List<T> boundries = new ArrayList<>(boundrySet);
    String property = orig.getPropertyName();
    FilterPredicate lower =
        new FilterPredicate(property, orig.getLowerBound().getOperator(), boundries.get(0));
    for (int i = 1; i < boundries.size() - 1; i++) {
      result.add(new Range(lower, new FilterPredicate(property, LESS_THAN, boundries.get(i))));
      lower = new FilterPredicate(property, GREATER_THAN_OR_EQUAL, boundries.get(i));
    }
    result.add(new Range(lower, new FilterPredicate(property, orig.getUpperBound().getOperator(),
        boundries.get(boundries.size() - 1))));
    return result;
  }

  private FilterPredicate findFirstPredicate(String namespace, String kind,
      List<Filter> equalityFilters, String propertyName, SortDirection direction) {
    String property = propertyName == null ? KEY_RESERVED_PROPERTY : propertyName;
    Query q = createQuery(namespace, kind).addSort(property, direction);
    if (!equalityFilters.isEmpty()) {
      if (equalityFilters.size() == 1) {
        q.setFilter(equalityFilters.get(0));
      } else {
        q.setFilter(new CompositeFilter(AND, equalityFilters));
      }
    }
    Iterator<Entity> iter = datastore.prepare(q).asIterator(withLimit(1));
    if (!iter.hasNext()) {
      return null;
    }
    Object object;
    if (propertyName == null) {
      object = iter.next().getKey();
    } else {
      object = iter.next().getProperty(propertyName);
    }
    return new FilterPredicate(property,
        direction == DESCENDING ? LESS_THAN_OR_EQUAL : GREATER_THAN_OR_EQUAL, object);
  }

  @VisibleForTesting
  static SortedSet<Long> splitRange(long start, long end, int numSplits) {
    SortedSet<Long> result = new TreeSet<>();
    result.add(start);
    long range = end - start;
    for (int i = 1; i < numSplits; i++) {
      double fraction = i / (double) numSplits;
      result.add(start + Math.round(range * fraction));
    }
    result.add(end);
    return result;
  }

  private static SortedSet<Double> splitRange(double start, double end, int numSplits) {
    SortedSet<Double> result = new TreeSet<>();
    result.add(start);
    double range = end - start;
    for (int i = 1; i < numSplits; i++) {
      double fraction = i / (double) numSplits;
      result.add(start + range * fraction);
    }
    result.add(end);
    return result;
  }
}
