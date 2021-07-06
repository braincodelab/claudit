from gcp.datasets import create_dataset
from gcp.dicom import create_dicom_store, import_dicom_instance

# from deid.dummy import


class DummyDataset:
    def __init__(
        self,
        project_id="automated-imaging-development",
        cloud_region="northamerica-northeast1",
        dataset_id="ich",
        dicom_store_id="mistie_3",
        content_uri="bios-data/m3_raw_redo/**.dcm",
    ):

        self.project_id = project_id
        self.cloud_region = cloud_region
        self.dataset_id = dataset_id
        self.dicom_store_id = dicom_store_id

    def create_dummy_dataset(self):

        create_dataset(self.project_id, self.cloud_region, self.dataset_id)

    def create_dicom_store(self):

        create_dicom_store(
            self.project_id, self.cloud_region, self.dataset_id, self.dicom_store_id
        )

    def import_dicom_instances(self):

        import_dicom_instance(
            self.project_id,
            self.cloud_region,
            self.dataset_id,
            self.dicom_store_id,
            self.content_uri,
        )
