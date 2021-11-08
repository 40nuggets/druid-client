<?php
declare(strict_types=1);

namespace Level23\Druid\InputSources;

use Level23\Druid\Interval\IntervalInterface;

class InlineInputSource implements InputSourceInterface
{
    /**
     * @var string
     */
    protected $data;

    /**
     * InlineInputSource constructor.
     *
     * @param string $data
     */
    public function __construct(string $data)
    {
        $this->data = $data;
    }

    public function toArray(): array
    {
        return [
            'type'       => 'inline',
            'data'       => $this->data,
        ];
    }
}