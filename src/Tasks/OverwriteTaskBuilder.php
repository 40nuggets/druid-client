<?php
declare(strict_types=1);

namespace Level23\Druid\Tasks;

use Closure;
use InvalidArgumentException;
use Level23\Druid\DruidClient;
use Level23\Druid\InputSources\InlineInputSource;
use Level23\Druid\Types\DataType;
use Level23\Druid\Context\TaskContext;
use Level23\Druid\Concerns\HasInterval;
use Level23\Druid\Concerns\HasAggregations;
use Level23\Druid\Concerns\HasTuningConfig;
use Level23\Druid\Transforms\TransformSpec;
use Level23\Druid\Transforms\TransformBuilder;
use Level23\Druid\Concerns\HasQueryGranularity;
use Level23\Druid\InputSources\DruidInputSource;
use Level23\Druid\Collections\IntervalCollection;
use Level23\Druid\Concerns\HasSegmentGranularity;
use Level23\Druid\Collections\TransformCollection;
use Level23\Druid\Granularities\UniformGranularity;
use Level23\Druid\Collections\AggregationCollection;
use Level23\Druid\Granularities\ArbitraryGranularity;

class OverwriteTaskBuilder extends TaskBuilder
{
    use HasInterval, HasTuningConfig;

    /**
     * The data source where we will write to.
     *
     * @var string
     */
    protected $dataSource;

    /**
     * @var string|null
     */
    protected $inputSourceType;

    /**
     * @var bool
     */
    protected $rollup = false;

    /**
     * Whether or not this task should be executed parallel.
     *
     * @var bool
     */
    protected $parallel = false;

    /**
     * @car string
     */
    protected $timeColumn = '__time';

    /**
     * @var string
     */
    protected $data;

    /**
     * IndexTaskBuilder constructor.
     *
     * @param \Level23\Druid\DruidClient $client
     * @param string                     $toDataSource    Data source where the data will be imported in.
     * @param string|null                $inputSourceType The type of InputSource to use (where to retrieve the data
     *                                                    from).
     */
    public function __construct(DruidClient $client, string $toDataSource, string $inputSourceType = null)
    {
        $this->client          = $client;
        $this->dataSource      = $toDataSource;
        $this->inputSourceType = $inputSourceType;
    }

    public function setData(string $data): OverwriteTaskBuilder
    {
        $this->data = $data;

        return $this;
    }


    public function setTimeColumn(string $timeColumn): OverwriteTaskBuilder
    {
        $this->timeColumn = $timeColumn;

        return $this;
    }

    /**
     * @param \Level23\Druid\Context\TaskContext|array $context
     *
     * @return \Level23\Druid\Tasks\TaskInterface
     * @throws \Level23\Druid\Exceptions\QueryResponseException
     */
    protected function buildTask($context): TaskInterface
    {
        if (is_array($context)) {
            $context = new TaskContext($context);
        }

        switch ($this->inputSourceType) {
            case DruidInputSource::class:
                $fromDataSource = $this->fromDataSource ?? $this->dataSource;

                // First, validate the given from and to. Make sure that these
                // match the beginning and end of an interval.
                $properties = $context->toArray();
                if (empty($properties['skipIntervalValidation'])) {
                    $this->validateInterval($fromDataSource, $this->interval);
                }

                $inputSource = new DruidInputSource($fromDataSource, $this->interval);
                break;
            case InlineInputSource::class:
                if (!$this->data) {
                    throw new InvalidArgumentException(
                        'InlineInputSource chosen without providing data'
                    );
                }
                $inputSource = new InlineInputSource($this->data);
                break;
            default:
                throw new InvalidArgumentException(
                    'No InputSource known. Currently we only support re-indexing (DruidInputSource).'
                );
        }

        $task = new OverwriteTask(
            $this->dataSource,
            $this->timeColumn,
            $inputSource,
            $this->tuningConfig,
            $context
        );

        if ($this->parallel) {
            $task->setParallel($this->parallel);
        }

        return $task;
    }

    /**
     * Enable rollup mode
     *
     * @return $this
     */
    public function rollup(): OverwriteTaskBuilder
    {
        $this->rollup = true;

        return $this;
    }

    /**
     * Execute this index task as parallel batch.
     */
    public function parallel(): OverwriteTaskBuilder
    {
        $this->parallel = true;

        return $this;
    }
}
