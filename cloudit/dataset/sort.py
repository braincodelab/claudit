import pydicom as pd
from pydicom.filereader import InvalidDicomError

import os
import shutil
import time


class DICOMSort:

    """

    see https://github.com/pieper/dicomsort

    """

    def __init__(
        self,
        study_name="%Modality-%StudyDescription-%StudyDate".replace("/", ""),
        series_name="%SeriesDescription-%SeriesNumber".replace("/", ""),
        instance_name="%Modality-%SeriesDescription-%SeriesNumber-%InstanceNumber.dcm".replace(
            "/", ""
        ),
    ):

        self.renamed_files = {}
        self.file_structure = "/" + study_name + "/" + series_name + "/" + instance_name

    def rename_files(self, data_path, dest_path, verbose=True):

        self.data_path = data_path
        self.dest_path = dest_path

        files_renamed = 0
        files_skipped = 0

        if verbose:
            print("Creating file list ...")
            timestamp = time.time()

        dicom_files = []

        for root, _, files in os.walk(self.data_path):
            for file in files:
                file = os.path.join(root, file)
                dicom_files.append(file)

        if verbose:
            print("Found ", len(dicom_files), " files, now sorting ...")

        for file in dicom_files:

            if self.rename_file(file):
                files_renamed += 1
            else:
                files_skipped += 1

        if verbose:
            print("Completed in {0} seconds".format(int(time.time() - timestamp)))

        return True

    def rename_file(self, file):

        try:
            ds = pd.read_file(file, stop_before_pixels=True)
        except (IOError, os.error) as why:
            print(
                "pydicom.read_file() IO error on file %s, exception %s"
                % (file, str(why))
            )
            return False
        except InvalidDicomError:
            return False
        except KeyError:  # handles dicomdir file errors
            return False

        path = self.dicom_pathfinder(ds)

        if os.path.exists(path):
            print("Target file exists - pattern is not unique")

        target_path = os.path.dirname(path)
        target_filename = os.path.basename(path)

        if not os.path.exists(target_path):
            os.makedirs(target_path)
            print("making:", target_path)

        try:
            shutil.copyfile(file, path)

        except (IOError, os.error) as why:
            print(
                "Dicom file copy IO error on output pathname >%s< Exception >%s<"
                % (path, str(why))
            )

            # keep track of files and new directories
        if target_path in self.renamed_files:
            self.renamed_files[target_path].append(target_filename)
        else:
            self.renamed_files[target_path] = [
                target_filename,
            ]
        return True

    def dicom_pathfinder(self, ds):
        replacements = {}
        form, keys = self.format_pattern()

        for key in keys:
            if hasattr(ds, key):
                value = ds.__getattr__(key)
            else:
                value = ""

            if key.endswith("Time"):
                try:
                    if str(value)[str(value).find(".") + 1] == "000000":
                        value = str(value)[: str(value).find(".")]
                except IndexError:
                    try:
                        value = str(value)[:4]
                    except IndexError:
                        value = str(value)

            try:
                replacements[key] = self.safe_filename(str(value))

            except UnicodeEncodeError as why:
                print("Encoding target path failed. Exception: %s" % why)
                value = "Unknown_%s_" % key
                replacements[key] = self.safe_filename(str(value))
            # else:
            #    replacements[key] = str(value)

        return form % replacements

    def safe_filename(self, filename):
        """Remove any potentially dangerous or confusing characters from
        the file name by mapping them to reasonable subsitutes"""
        underscores = r"""+`~!@#$%^&*(){}[]/=\|<>,.":' """
        safe_name = ""
        for c in filename:
            if c in underscores:
                c = "_"
            safe_name += c
        return safe_name

    def format_pattern(self):

        keys = []
        form = ""

        p = self.dest_path + self.file_structure
        end = len(p)
        i = 0

        while i < end:
            c = p[i]

            if c == "%":
                form += "%("
                i += 1
                key = ""

                while True:
                    c = p[i]
                    i += 1
                    if not c.isalpha() or i >= end:
                        form += ")s"
                        i -= 1
                        break
                    else:
                        form += c
                        key += c
                keys.append(key)
            else:
                form += c
                i += 1
        return (form, keys)
