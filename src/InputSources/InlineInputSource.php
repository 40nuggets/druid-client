<?php
declare(strict_types=1);

namespace Level23\Druid\InputSources;

use Level23\Druid\Interval\IntervalInterface;

class InlineInputSource implements InputSourceInterface
{
    /**
     * @var array
     */
    protected $data;

    /**
     * InlineInputSource constructor.
     *
     * @param array $data
     */
    public function __construct(array $data)
    {
        $this->data = $data;
    }

    public function toArray(): array
    {
        $encodedData = array_map(function ($elem) {
            return json_encode($elem);
        }, $this->data);
        return [
            'type'       => 'inline',
            'data'       => implode("\n", $encodedData),
        ];
    }
}