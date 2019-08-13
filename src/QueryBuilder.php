<?php
declare(strict_types=1);

namespace Level23\Druid;

use InvalidArgumentException;
use Level23\Druid\Collections\AggregationCollection;
use Level23\Druid\Collections\DimensionCollection;
use Level23\Druid\Collections\IntervalCollection;
use Level23\Druid\Collections\PostAggregationCollection;
use Level23\Druid\Concerns\HasAggregations;
use Level23\Druid\Concerns\HasDimensions;
use Level23\Druid\Concerns\HasFilter;
use Level23\Druid\Concerns\HasHaving;
use Level23\Druid\Concerns\HasIntervals;
use Level23\Druid\Concerns\HasLimit;
use Level23\Druid\Concerns\HasPostAggregations;
use Level23\Druid\Context\GroupByQueryContext;
use Level23\Druid\Context\TimeSeriesQueryContext;
use Level23\Druid\Context\TopNQueryContext;
use Level23\Druid\Dimensions\DimensionInterface;
use Level23\Druid\Limits\LimitInterface;
use Level23\Druid\Queries\GroupByQuery;
use Level23\Druid\Queries\QueryInterface;
use Level23\Druid\Queries\TimeSeriesQuery;
use Level23\Druid\Queries\TopNQuery;
use Level23\Druid\Types\Granularity;
use Level23\Druid\Types\OrderByDirection;

class QueryBuilder
{
    use HasFilter, HasHaving, HasDimensions, HasAggregations, HasIntervals, HasLimit, HasPostAggregations;

    /**
     * @var \Level23\Druid\DruidClient
     */
    protected $client;

    /**
     * @var string
     */
    protected $dataSource;

    /**
     * @var string|\Level23\Druid\Types\Granularity
     */
    protected $granularity;

    /**
     * QueryBuilder constructor.
     *
     * @param \Level23\Druid\DruidClient              $client
     * @param string                                  $dataSource
     * @param string|\Level23\Druid\Types\Granularity $granularity
     */
    public function __construct(DruidClient $client, string $dataSource, $granularity = 'all')
    {
        if (is_string($granularity) && !Granularity::isValid($granularity)) {
            throw new InvalidArgumentException(
                'The given granularity is invalid: ' . $granularity . '. ' .
                'Allowed are: ' . implode(',', Granularity::values())
            );
        }

        $this->client      = $client;
        $this->dataSource  = $dataSource;
        $this->granularity = $granularity;
    }

    /**
     * Execute a druid query. We will try to detect the best possible query type possible.
     *
     * @param array $context
     *
     * @return array
     * @throws \Level23\Druid\Exceptions\DruidException
     * @throws \Level23\Druid\Exceptions\DruidQueryException
     */
    public function execute(array $context = []): array
    {
        $query = $this->buildQueryAutomatic($context);

        return $this->client->executeDruidQuery($query);
    }

    /**
     * Return the query as a JSON string
     *
     * @param array $context
     *
     * @return string
     */
    public function toJson(array $context = []): string
    {
        $query = $this->buildQueryAutomatic($context);

        $json = json_encode($query->getQuery(), JSON_PRETTY_PRINT);
        if ($json === false) {
            return "";
        }

        return $json;
    }

    /**
     * Return the query as an array
     *
     * @param array $context
     *
     * @return array
     */
    public function toArray(array $context = []): array
    {
        $query = $this->buildQueryAutomatic($context);

        return $query->getQuery();
    }

    /**
     * Execute a timeseries query.
     *
     * @param array $context
     *
     * @return array
     * @throws \Level23\Druid\Exceptions\DruidException
     * @throws \Level23\Druid\Exceptions\DruidQueryException
     */
    public function timeseries(array $context = [])
    {
        $query = $this->buildTimeSeriesQuery($context);

        return $this->client->executeDruidQuery($query);
    }

    /**
     * Execute a topN query.
     *
     * @param array $context
     *
     * @return array
     * @throws \Level23\Druid\Exceptions\DruidException
     * @throws \Level23\Druid\Exceptions\DruidQueryException
     */
    public function topN(array $context = [])
    {
        $query = $this->buildTopNQuery($context);

        return $this->client->executeDruidQuery($query);
    }

    /**
     * Return the group by query
     *
     * @param array $context
     *
     * @return array
     * @throws \Level23\Druid\Exceptions\DruidException
     * @throws \Level23\Druid\Exceptions\DruidQueryException
     */
    public function groupBy(array $context = [])
    {
        $query = $this->buildGroupByQuery($context);

        return $this->client->executeDruidQuery($query);
    }




    //<editor-fold desc="Protected methods">

    /**
     * Build a timeseries query.
     *
     * @param array $context
     *
     * @return TimeSeriesQuery
     */
    protected function buildTimeSeriesQuery(array $context = []): TimeSeriesQuery
    {
        $query = new TimeSeriesQuery(
            $this->dataSource,
            new IntervalCollection(...$this->intervals),
            $this->granularity
        );

        if (count($context) > 0) {
            $query->setContext(new TimeSeriesQueryContext($context));
        }

        if ($this->filter) {
            $query->setFilter($this->filter);
        }

        if (count($this->aggregations) > 0) {
            $query->setAggregations(new AggregationCollection(...$this->aggregations));
        }

        if (count($this->postAggregations) > 0) {
            $query->setPostAggregations(new PostAggregationCollection(...$this->postAggregations));
        }

        if ($this->limit) {
            $orderByCollection = $this->limit->getOrderByCollection();
            if (count($orderByCollection) == 1) {
                /** @var \Level23\Druid\OrderBy\OrderByInterface $orderBy */
                $orderBy = $orderByCollection[0];
                if ($orderBy->getDimension() == '__time' && $orderBy->getDirection() === OrderByDirection::DESC()) {
                    $query->setDescending(true);
                }
            }
        }

        return $query;
    }

    /**
     * Build a topN query.
     *
     * @param array $context
     *
     * @return TopNQuery
     */
    public function buildTopNQuery(array $context = []): TopNQuery
    {
        if (!$this->limit instanceof LimitInterface) {
            throw new InvalidArgumentException('You should specify a limit to make use of a top query');
        }

        $orderByCollection = $this->limit->getOrderByCollection();
        $orderBy           = $orderByCollection[0];

        /** @var \Level23\Druid\OrderBy\OrderByInterface $orderBy */
        $query = new TopNQuery(
            $this->dataSource,
            new IntervalCollection(...$this->intervals),
            $this->dimensions[0],
            $this->limit->getLimit(),
            $orderBy->getDimension(),
            $this->granularity
        );

        if (count($this->aggregations) > 0) {
            $query->setAggregations(new AggregationCollection(...$this->aggregations));
        }

        if (count($this->postAggregations) > 0) {
            $query->setPostAggregations(new PostAggregationCollection(...$this->postAggregations));
        }

        if (count($context) > 0) {
            $query->setContext(new TopNQueryContext($context));
        }

        if ($this->filter) {
            $query->setFilter($this->filter);
        }

        return $query;
    }

    /**
     * Build the group by query
     *
     * @param array $context
     *
     * @return GroupByQuery
     */
    public function buildGroupByQuery(array $context = []): GroupByQuery
    {
        $query = new GroupByQuery(
            $this->dataSource,
            new DimensionCollection(...$this->dimensions),
            new IntervalCollection(...$this->intervals),
            new AggregationCollection(...$this->aggregations),
            $this->granularity
        );

        if (count($context) > 0) {
            $query->setContext(new GroupByQueryContext($context));
        }

        if (count($this->postAggregations) > 0) {
            $query->setPostAggregations(new PostAggregationCollection(...$this->postAggregations));
        }

        if ($this->filter) {
            $query->setFilter($this->filter);
        }

        if ($this->limit) {
            $query->setLimit($this->limit);
        }

        // @todo : subtotalsSpec

        if ($this->having) {
            $query->setHaving($this->having);
        }

        return $query;
    }

    /**
     * Return the query automatically detected based on the requested data.
     *
     * @param array $context
     *
     * @return \Level23\Druid\Queries\QueryInterface
     */
    protected function buildQueryAutomatic(array $context = []): QueryInterface
    {
        $type = 'groupBy';

        /**
         * If we only have "grouped" by __time, then we can use a time series query.
         * This is preferred, because it's a lot faster then doing a group by query.
         */
        if (count($this->dimensions) == 1) {
            /** @var DimensionInterface $dimension */
            $config = $this->dimensions[0]->getDimension();

            // did we only retrieve the time dimension?
            if ($config['dimension'] == '__time') {
                $type = 'timeseries';
            } // Check if we can use a topN query.
            elseif (
                $this->limit
                && $this->limit->getLimit() != self::$DEFAULT_MAX_LIMIT
                && count($this->limit->getOrderByCollection()) == 1
            ) {
                // We can use a topN!
                $type = 'topN';
            }
        }

        switch ($type) {
            case 'timeseries':
                $query = $this->buildTimeSeriesQuery($context);
                break;

            case 'topN':
                $query = $this->buildTopNQuery($context);
                break;

            default:
            case 'groupBy':
                $query = $this->buildGroupByQuery($context);
                break;
        }

        return $query;
    }
    //</editor-fold>
}
