<?php
declare(strict_types=1);

namespace Level23\Druid\InputSources;

class S3InputSource implements InputSourceInterface
{
    /**
     * @var array
     */
    protected $uris;

    /**
     * S3InputSource constructor.
     *
     * @param array $uris
     */
    public function __construct(array $uris)
    {
        $this->uris = $uris;
    }

    public function toArray(): array
    {
        return [
            'type' => 's3',
            'uris' => json_encode(array_values($this->uris)),
            'properties' => [
                "accessKeyId" => [
                    "type" => "environment",
                    "variable" => "druid_s3_accessKey"
                ],
                "secretAccessKey" => [
                    "type" => "environment",
                    "variable" => "druid_s3_secretKey"
                ]
            ]
        ];
    }
}