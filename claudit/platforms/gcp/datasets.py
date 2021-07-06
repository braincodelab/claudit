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


def create_dataset(project_id, cloud_region, dataset_id):
    """Creates a dataset."""
    client = get_client()
    dataset_parent = "projects/{}/locations/{}".format(project_id, cloud_region)

    request = (
        client.projects()
        .locations()
        .datasets()
        .create(parent=dataset_parent, body={}, datasetId=dataset_id)
    )

    response = request.execute()
    print("Created dataset: {}".format(dataset_id))
    return response


def delete_dataset(project_id, cloud_region, dataset_id):
    """Deletes a dataset."""
    client = get_client()
    dataset_name = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )

    request = client.projects().locations().datasets().delete(name=dataset_name)

    response = request.execute()
    print("Deleted dataset: {}".format(dataset_id))
    return response


def get_dataset(project_id, cloud_region, dataset_id):
    """Gets any metadata associated with a dataset."""
    client = get_client()
    dataset_name = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )

    datasets = client.projects().locations().datasets()
    dataset = datasets.get(name=dataset_name).execute()

    print("Name: {}".format(dataset.get("name")))
    print("Time zone: {}".format(dataset.get("timeZone")))

    return dataset


def list_datasets(project_id, cloud_region):
    """Lists the datasets in the project."""
    client = get_client()
    dataset_parent = "projects/{}/locations/{}".format(project_id, cloud_region)

    datasets = (
        client.projects()
        .locations()
        .datasets()
        .list(parent=dataset_parent)
        .execute()
        .get("datasets", [])
    )

    for dataset in datasets:
        print(
            "Dataset: {}\nTime zone: {}".format(
                dataset.get("name"), dataset.get("timeZone")
            )
        )

    return datasets


def patch_dataset(project_id, cloud_region, dataset_id, time_zone):
    """Updates dataset metadata."""
    client = get_client()
    dataset_parent = "projects/{}/locations/{}".format(project_id, cloud_region)
    dataset_name = "{}/datasets/{}".format(dataset_parent, dataset_id)

    # Sets the time zone to GMT
    patch = {"timeZone": time_zone}

    request = (
        client.projects()
        .locations()
        .datasets()
        .patch(name=dataset_name, updateMask="timeZone", body=patch)
    )

    response = request.execute()
    print("Patched dataset {} with time zone: {}".format(dataset_id, time_zone))
    return response


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


def get_dataset_iam_policy(project_id, cloud_region, dataset_id):
    """Gets the IAM policy for the specified dataset."""
    client = get_client()
    dataset_name = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )

    request = (
        client.projects().locations().datasets().getIamPolicy(resource=dataset_name)
    )
    response = request.execute()

    print("etag: {}".format(response.get("name")))
    return response


def set_dataset_iam_policy(
    project_id, cloud_region, dataset_id, member, role, etag=None
):
    """Sets the IAM policy for the specified dataset.
    A single member will be assigned a single role. A member can be any of:
    - allUsers, that is, anyone
    - allAuthenticatedUsers, anyone authenticated with a Google account
    - user:email, as in 'user:somebody@example.com'
    - group:email, as in 'group:admins@example.com'
    - domain:domainname, as in 'domain:example.com'
    - serviceAccount:email,
        as in 'serviceAccount:my-other-app@appspot.gserviceaccount.com'
    A role can be any IAM role, such as 'roles/viewer', 'roles/owner',
    or 'roles/editor'
    """
    client = get_client()
    dataset_name = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )

    policy = {"bindings": [{"role": role, "members": [member]}]}

    if etag is not None:
        policy["etag"] = etag

    request = (
        client.projects()
        .locations()
        .datasets()
        .setIamPolicy(resource=dataset_name, body={"policy": policy})
    )
    response = request.execute()

    print("etag: {}".format(response.get("name")))
    print("bindings: {}".format(response.get("bindings")))
    return response
