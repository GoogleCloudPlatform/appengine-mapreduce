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

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilter;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Rating;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * Splits an arbitrary datastore query. This is used by {@link DatastoreInput} to shard queries.
 * This is done in one of two ways:
 *
 *  1. If the query contains an inequality filter, the lower and upper bounds are determined (this
 * may involve querying the datastore) then the range is split naively. This works well when the
 * property that is being queried on is uniformly distributed.
 *
 *  2. If the query does not contain an inequality filter. The query will be partitioned by the
 * entity key. This is done by using the "__scatter__" property to get a random sample of the
 * keyspace and partitioning based on that. This can result in a poor distribution if there are
 * equality filters on the query that bias the selection with respect to certain regions of
 * keyspace.
 *
 *  The following clauses are not supported by this class: An inequality filter of unsupported type.
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
    SortedSet<T> getSplitPoints(Range range, int numSplits);
  }

  private static class DoubleSplitter implements Splitter<Double> {
    @Override
    public SortedSet<Double> getSplitPoints(Range range, int numSplits) {
      return splitRange(((Number) range.getLowerBound().getValue()).doubleValue(),
          ((Number) range.getUpperBound().getValue()).doubleValue(), numSplits);
    }
  }

  private static class LongSplitter implements Splitter<Long> {
    @Override
    public SortedSet<Long> getSplitPoints(Range range, int numSplits) {
      return splitRange(((Number) range.getLowerBound().getValue()).longValue(),
          ((Number) range.getUpperBound().getValue()).longValue(), numSplits);
    }
  }

  private static class DateSplitter implements Splitter<Long> {
    @Override
    public SortedSet<Long> getSplitPoints(Range range, int numSplits) {
      return splitRange(((Date) range.getLowerBound().getValue()).getTime(),
          ((Date) range.getUpperBound().getValue()).getTime(), numSplits);
    }
  }

  private static class RatingSplitter implements Splitter<Long> {
    @Override
    public SortedSet<Long> getSplitPoints(Range range, int numSplits) {
      return splitRange(((Rating) range.getLowerBound().getValue()).getRating(),
          ((Rating) range.getUpperBound().getValue()).getRating(), numSplits);
    }
  }

  private static final Logger logger = Logger.getLogger(DatastoreShardStrategy.class.getName());

  private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
      ConcurrentModificationException.class, DatastoreTimeoutException.class,
      DatastoreFailureException.class).abortOn(EntityNotFoundException.class).build();

  private static final Map<Class<?>, Splitter<?>> typeMap =
      ImmutableMap.<Class<?>, Splitter<?>>builder()
          // Doubles
          .put(Float.class, new DoubleSplitter()).put(Double.class, new DoubleSplitter())
          // Integers
          .put(Byte.class, new LongSplitter())
          .put(Short.class, new LongSplitter())
          .put(Integer.class, new LongSplitter())
          .put(Long.class, new LongSplitter())
          .put(Date.class, new DateSplitter())
          .put(Rating.class, new RatingSplitter())
          .build();

  private final DatastoreService datastore;


  DatastoreShardStrategy(DatastoreService datastoreService) {
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
    List<Range> ranges;
    if (property == null) {
      ranges = getScatterSplitPoints(query.getNamespace(), query.getKind(), numSegments);
    } else {
      if (range.getUpperBound() == null) {
        FilterPredicate predicate = findFirstPredicate(query.getNamespace(), query.getKind(),
            equalityFilters, property, DESCENDING);
        if (predicate == null) {
          return ImmutableList.of(query);
        }
        range.setUpperBound(predicate);
      }
      if (range.getLowerBound() == null) {
        FilterPredicate predicate = findFirstPredicate(query.getNamespace(), query.getKind(),
            equalityFilters, property, ASCENDING);
        if (predicate == null) {
          return ImmutableList.of(query);
        }
        range.setLowerBound(predicate);
      }
      ranges = boundriesToRanges(range, getSplitter(range).getSplitPoints(range, numSegments));
    }
    return toQueries(query, equalityFilters, ranges);
  }

  /**
   * Uses the scatter property to distribute ranges to segments.
   *
   *  A random scatter property is added to 1 out of every 512 entities see:
   * http://code.google.com/p/appengine-mapreduce/wiki/ScatterPropertyImplementation
   *
   *  Retrieving the entities with the highest scatter values provides a random sample of entities.
   * Because they are randomly selected, their distribution in keyspace should be the same as other
   * entities.
   *
   *  Looking at Keyspace, It looks something like this:
   * |---*------*------*---*--------*-----*--------*--| Where "*" is a scatter entity and "-" is
   * some other entity.
   *
   *  So once sample entities are obtained them by key allows them to serve as boundaries between
   * ranges of keyspace.
   */
  private List<Range> getScatterSplitPoints(String namespace, String kind, final int numSegments) {
    Query query = createQuery(namespace, kind).addSort(SCATTER_RESERVED_PROPERTY).setKeysOnly();
    List<Key> splitPoints = sortKeys(runQuery(query, numSegments - 1));
    List<Range> result = new ArrayList<>(splitPoints.size() + 1);
    FilterPredicate lower = null;
    for (Key k : splitPoints) {
      result.add(new Range(lower, new FilterPredicate(KEY_RESERVED_PROPERTY, LESS_THAN, k)));
      lower = new FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN_OR_EQUAL, k);
    }
    result.add(new Range(lower, null));
    logger.info("Requested " + numSegments + " segments, retrieved " + result.size());
    return result;
  }

  private List<Key> sortKeys(List<Entity> entities) {
    List<Key> result = new ArrayList<>(entities.size());
    for (Entity e : entities) {
      result.add(e.getKey());
    }
    Collections.sort(result);
    return result;
  }

  private List<Query> toQueries(Query query, List<Filter> equalityFilters, List<Range> split) {
    List<Query> result = new ArrayList<>(split.size());
    for (Range r : split) {
      Query subQuery = SerializationUtil.clone(query);
      Builder<Filter> b = ImmutableList
          .<Filter>builder()
          .addAll(equalityFilters);
      if (r.getLowerBound() != null) {
        b.add(r.getLowerBound());
      }
      if (r.getUpperBound() != null) {
        b.add(r.getUpperBound());
      }
      ImmutableList<Filter> filters = b.build();
      if (!filters.isEmpty()) {
        if (filters.size() > 1) {
          subQuery.setFilter(new CompositeFilter(AND, filters));
        } else {
          subQuery.setFilter(filters.get(0));
        }
      }
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
    Query q = createQuery(namespace, kind).addSort(propertyName, direction);
    if (!equalityFilters.isEmpty()) {
      if (equalityFilters.size() == 1) {
        q.setFilter(equalityFilters.get(0));
      } else {
        q.setFilter(new CompositeFilter(AND, equalityFilters));
      }
    }
    List<Entity> item = runQuery(q, 1);
    if (item.isEmpty()) {
      return null;
    }
    return new FilterPredicate(propertyName,
        direction == DESCENDING ? LESS_THAN_OR_EQUAL : GREATER_THAN_OR_EQUAL, item.get(0));
  }

  private List<Entity> runQuery(Query q, final int limit) {
    final PreparedQuery preparedQuery = datastore.prepare(q);
    return RetryHelper.runWithRetries(new Callable<List<Entity>>() {
      @Override
      public List<Entity> call() {
        List<Entity> list = preparedQuery.asList(withLimit(limit));
        list.size(); // Forces the loading of all the data.
        return list;
      }
    }, RetryParams.getDefaultInstance(), EXCEPTION_HANDLER);
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

  @VisibleForTesting
  static SortedSet<Double> splitRange(double start, double end, int numSplits) {
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
