import os
import json
from googleapiclient import discovery


def get_client():
    """Returns an authorized API client by discovering the Healthcare API and
    creating a service object using the service account credentials in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    api_version = "v1"
    service_name = "healthcare"

    return discovery.build(service_name, api_version)


def deidentify_dataset(
    project_id, cloud_region, dataset_id, destination_dataset_id, keeplist_tags
):
    """Creates a new dataset containing de-identified data
    from the source dataset.
    """
    client = get_client()
    source_dataset = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )
    destination_dataset = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, destination_dataset_id
    )

    body = {
        "destinationDataset": destination_dataset,
        "config": {
            "dicom": {
                "keepList": {
                    "tags": [
                        "Columns",
                        "NumberOfFrames",
                        "PixelRepresentation",
                        "MediaStorageSOPClassUID",
                        "MediaStorageSOPInstanceUID",
                        "Rows",
                        "SamplesPerPixel",
                        "BitsAllocated",
                        "HighBit",
                        "PhotometricInterpretation",
                        "BitsStored",
                        "PatientID",
                        "TransferSyntaxUID",
                        "SOPInstanceUID",
                        "StudyInstanceUID",
                        "SeriesInstanceUID",
                        "PixelData",
                    ]
                }
            }
        },
    }

    request = (
        client.projects()
        .locations()
        .datasets()
        .deidentify(sourceDataset=source_dataset, body=body)
    )

    response = request.execute()
    print(
        "Data in dataset {} de-identified."
        "De-identified data written to {}".format(dataset_id, destination_dataset_id)
    )
    return response
