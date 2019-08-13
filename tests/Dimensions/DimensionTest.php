<?php
declare(strict_types=1);

namespace tests\Level23\Druid\Dimensions;

use Level23\Druid\Dimensions\Dimension;
use Level23\Druid\Extractions\ExtractionInterface;
use Level23\Druid\Extractions\RegexExtraction;
use tests\TestCase;

class DimensionTest extends TestCase
{
    public function dataProvider(): array
    {
        $extr = new RegexExtraction("^([a-z]+)$");

        return [
            ["name", "full_name", "string", null, false],
            ["name", null, "STRING", null, false],
            ["name", "full_name", "double", null, true],
            ["name", "full_name", "whatever", null, true],
            ["name", "full_name", "whatever", null, true],
            ["name", "full_name", "", null, false],
            ["name", "full_name", "", $extr, false],
        ];
    }

    /**
     * @dataProvider dataProvider
     *
     * @param string                   $dimension
     * @param string|null              $outputName
     * @param string                   $type
     * @param ExtractionInterface|null $extractionFunction
     * @param bool                     $expectException
     */
    public function testDimension(
        string $dimension,
        ?string $outputName,
        string $type,
        ?ExtractionInterface $extractionFunction,
        bool $expectException
    ) {
        if ($expectException) {
            $this->expectException(\InvalidArgumentException::class);
        }

        if (!empty($type) || $extractionFunction !== null) {
            $dimensionObj = new Dimension($dimension, $outputName, $type, $extractionFunction);
        } else {
            $dimensionObj = new Dimension($dimension, $outputName);
        }
        $expected = [
            'type'       => ($extractionFunction ? 'extraction' : 'default'),
            'dimension'  => $dimension,
            'outputName' => ($outputName ?: $dimension),
            'outputType' => strtolower($type ?: "string"),
        ];

        if ($extractionFunction) {
            $expected['extractionFn'] = $extractionFunction->getExtractionFunction();
        }

        $this->assertEquals($expected, $dimensionObj->getDimension());
    }
}