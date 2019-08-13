<?php

namespace tests\Level23\Druid\Aggregations;

use Level23\Druid\Aggregations\LastAggregator;
use Level23\Druid\Types\DataType;
use tests\TestCase;

class LastAggregatorTest extends TestCase
{
    public function dataProvider(): array
    {
        return [
            [DataType::LONG()],
            [DataType::DOUBLE()],
            [DataType::FLOAT()],
            [DataType::STRING(), true],
        ];
    }

    /**
     * @dataProvider  dataProvider
     *
     * @param DataType $type
     * @param bool     $expectException
     */
    public function testAggregator($type, bool $expectException = false)
    {
        if ($expectException) {
            $this->expectException(\InvalidArgumentException::class);
        }

        $aggregator = new LastAggregator('abc', 'dim123', $type);
        $this->assertEquals([
            'type'      => $type . 'Last',
            'name'      => 'dim123',
            'fieldName' => 'abc',
        ], $aggregator->getAggregator());

        $this->assertEquals('dim123', $aggregator->getOutputName());
    }
}