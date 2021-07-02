import os
import fnmatch
import pydicom
import pandas as pd
from pydicom.uid import PYDICOM_IMPLEMENTATION_UID
from pydicom._version import __version_info__
from datetime import datetime
from random import shuffle
from pathlib import Path

# Import ID reference table.
df_reftable = pd.read_excel('/Users/paulryu/BrainCodeLab/DICOM-ReAn/MISTIE_III_Link_File.xlsx')
# Set study path.
path_study = '/Volumes/LaCie-1626/DATA 2/Image Archive 2/MISTIE III/Anonymized_restructured/'
# Set export path.
path_export = '/Users/paulryu/BrainCodeLab/DICOM-ReAn/'

unwanted = ['Dose', 'Localizers', 'Scene', 'Save', 'MONTAGE', 'Processed', 'Protocol', 'SmartBrain']


def timestamp():
    return datetime.today().strftime("[%H:%M:%S] ")


def find_dcm(path, pt):
    print(timestamp(), "Initiated building a list of all DICOM files for", pt)
    filelist = []
    for root, _, filenames in os.walk(path):
        for f in fnmatch.filter(filenames, '*.dcm'):
            global unwanted
            if any(q in root for q in unwanted):
                pass
            else:
                filelist.append(os.path.join(root, f))
    print(timestamp(), "Successfully built a list of all DICOM files for", pt)
    return filelist


def find_datetime_i(files, pt):
    print(timestamp(), "Initiated searching for the earliest scan for", pt)
    datetimelist = []
    for f in files:
        dataset = pydicom.dcmread(f, specific_tags=[0x00080022, 0x00080023, 0x00080032, 0x00080033])
        if not hasattr(dataset, 'StudyDescription') or dataset.StudyDescription == "":
            dataset.StudyDescription = Path(f).parts[-3]
        if not hasattr(dataset, 'AcquisitionDate') or dataset.AcquisitionDate == "":
            dataset.AcquisitionDate = dataset.ContentDate
        if not hasattr(dataset, 'AcquisitionTime') or dataset.AcquisitionTime == "":
            dataset.AcquisitionTime = dataset.ContentTime
        try:
            datetimelist.append(datetime.strptime(dataset.AcquisitionDate + dataset.AcquisitionTime,
                                                  '%Y%m%d%H%M%S.%f'))
        except ValueError:
            datetimelist.append(datetime.strptime(dataset.AcquisitionDate + dataset.AcquisitionTime +
                                                  ".0", '%Y%m%d%H%M%S.%f'))
    datetime_initial = min(datetimelist)
    print(timestamp(), "Successfully found the earliest scan for", pt, ":", datetime_initial)
    return datetime_initial


def fnr_id(search_query, dataset):
    searched_row = df_reftable.loc[df_reftable['patient_ID'] == int(search_query)]
    id_pt = str(searched_row['patientID_NINDS'].item())
    id_site = str(searched_row['siteID_NINDS'].item())
    dataset.PatientName = dataset.ClinicalTrialSubjectID = id_pt
    dataset.ClinicalTrialSiteID = id_site
    dataset.PatientID = id_site + '-' + id_pt
    return id_pt + '-' + id_site


def callback_date(_, data_element):
    if data_element.VR == "DA" and data_element.tag != 'PatientBirthDate':
        global datetime_n
        data_element.value = datetime_n[:8]


def callback_time(_, data_element):
    if data_element.VR == "TM":
        data_element.value = datetime_n[8:]


def callback_remainder(dataset, data_element):
    keeplist = ['MediaStorageSOPClassUID',  # (0002, 0002)
                'MediaStorageSOPInstanceUID',  # (0002, 0003)
                'TransferSyntaxUID',  # (0002, 0010)
                'SOPClassUID',  # (0008, 0016)
                'SOPInstanceUID',  # (0008, 0018)
                'StudyDate',  # (0008, 0020)
                'SeriesDate',  # (0008, 0021)
                'AcquisitionDate',  # (0008, 0022)
                'ContentDate',  # (0008, 0023)
                'StudyTime',  # (0008, 0030)
                'SeriesTime',  # (0008, 0031)
                'AcquisitionTime',  # (0008, 0032)
                'ContentTime',  # (0008, 0033)
                'Modality',  # (0008, 0060)
                'SeriesDescription',  # (0008, 103E)
                'PatientName',  # (0010, 0010)
                'PatientID',  # (0010, 0020)
                'ClinicalTrialSiteID',  # (0012, 0030)
                'ClinicalTrialSubjectID',  # (0012, 0040)
                'SliceThickness',  # (0018, 0050)ß
                'SpacingBetweenSlices',  # (0018, 0088)
                'GantryDetectorTilt',  # (0018, 1120)
                'StudyInstanceUID',  # (0020, 000D)
                'SeriesInstanceUID',  # (0020, 000E)
                'NumberOfFrames',  # (0020, 0008)
                'ImagePositionPatient',  # (0020, 0032)
                'ImageOrientationPatient',  # (0020, 0037)
                'SliceLocation',  # (0020, 1041)
                'SamplesPerPixel',  # (0028, 0002)
                'PhotometricInterpretation',  # (0028, 0004)
                'Rows',  # (0028, 0010)
                'Columns',  # (0028, 0011)
                'PixelSpacing',  # (0028, 0030)
                'BitsAllocated',  # (0028, 0100)
                'BitsStored',  # (0028, 0101)
                'HighBit',  # (0028, 0102)
                'PixelRepresentation',  # (0028, 0103)
                'RescaleIntercept',  # (0028, 1052)
                'RescaleSlope',  # (0028, 1053)
                'RescaleType',  # (0028, 1054)
                'LossyImageCompression',  # (0028, 2110)
                'LossyImageCompressionMethod',  # (0028, 2114)
                'PixelData'  # (7FE0, 0010)
                ]
    if data_element.tag not in keeplist:
        del dataset[data_element.tag]


def countnum(stu, ser, sca, c, v, f):
    if c == 0:
        pass
    else:
        sca += 1
        if Path(v).parts[-2] != Path(f[c - 1]).parts[-2]:
            ser += 1
            sca = 1
        if Path(v).parts[-3] != Path(f[c - 1]).parts[-3]:
            stu += 1
            ser = 1
            sca = 1
    return stu, ser, sca


# Build list of patient folders with random order.
folders_pt = sorted(next(os.walk(path_study))[1])
folders_pt = [x for x in folders_pt if '-' in x]
# shuffle(folders_pt)

for x in folders_pt:
    # Find all DICOM files in the patient folder.
    path_files = os.path.join(path_study, x)
    files_dcm = find_dcm(path_files, x)

    # Find earliest scan date & time and filter DICOM files once more within metadata.
    datetime_i = find_datetime_i(files_dcm, x)
    count_study = count_series = count_scan = 1
    datetime_mint = datetime.strftime(datetime.today(), '%Y%m%d%H%M%S')

    for count, value in enumerate(files_dcm):
        ds = pydicom.dcmread(value)

        # Find patient ID on the reference table & replace patient name and ID with new NINDS values.
        pt_id = fnr_id(x[:4], ds)

        # Assign relative date & time to all date & time attributes.
        if not hasattr(ds, 'AcquisitionDate') or ds.AcquisitionDate == '':
            ds.AcquisitionDate = ds.ContentDate
        if not hasattr(ds, 'AcquisitionTime') or ds.AcquisitionTime == '':
            ds.AcquisitionTime = ds.ContentTime
        datetime_f = ds.AcquisitionDate + ds.AcquisitionTime
        try:
            datetime_f = datetime.strptime(datetime_f, '%Y%m%d%H%M%S.%f')
        except ValueError:
            datetime_f = datetime.strptime(datetime_f + ".0", '%Y%m%d%H%M%S.%f')
        datetime_d = datetime_f - datetime_i
        datetime_n = datetime.strftime(datetime(2000, 1, 1, 0, 0, 0, 0) + datetime_d, '%Y%m%d%H%M%S.%f')

        ds.walk(callback_date)
        ds.walk(callback_time)

        # Redact all remaining attributes.
        ds.walk(callback_remainder)

        # Assign count numbers for UID & folder/file naming.
        count_study, count_series, count_scan = countnum(count_study, count_series, count_scan, count, value, files_dcm)
        count_study_str, count_series_str, count_scan_str = str(count_study), str(count_series), str(count_scan)

        # Mint UIDs.
        uid_prefix = "9.9.999.9.9.999999.99.99." + str(datetime_mint) + "."
        ds.file_meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID  # "1.2.826.0.1.3680043.8.498.1"
        ds.file_meta.ImplementationVersionName = ('PYDICOM ' + ".".join(str(x) for x in __version_info__))
        ds.file_meta.MediaStorageSOPInstanceUID = uid_prefix + count_study_str + "." + count_series_str + "." + \
                                                  count_scan_str
        ds.SOPInstanceUID = uid_prefix + count_study_str + "." + count_series_str + "." + count_scan_str
        ds.SeriesInstanceUID = uid_prefix + count_study_str + "." + count_series_str
        ds.StudyInstanceUID = uid_prefix + count_study_str

        # Save DICOM files with proper naming scheme.
        if not hasattr(ds, 'SeriesDescription') or ds.SeriesDescription == "":
            ds.SeriesDescription = Path(value).parts[-2]
        ds.StudyDescription = Path(value).parts[-3]

        name_scan = ds.SOPInstanceUID + ".dcm"
        name_series = ds.SeriesDescription
        name_study = ds.StudyDescription
        product = os.path.join(path_export, pt_id, name_study, name_series, name_scan)

        # Save anonymized files.
        try:
            ds.save_as(product)
        except FileNotFoundError:
            Path(os.path.split(product)[0]).mkdir(parents=True, exist_ok=True)
            ds.save_as(product)

        # Output results.
        print(timestamp(), product, "was processed and saved.")