import os


def insert_tags(
    folder, tags, dummy="rocky94@!", data_path=data_path, dest_path=dest_path
):

    os.chdir(dcm_dir)
    studies = sorted(next(os.walk("."))[1])

    for j in range(len(studies)):
        # patient dicom study level
        study = os.path.join(dcm_dir, studies[j])
        os.chdir(study)
        all_series = sorted(next(os.walk("."))[1])

        for k in range(len(all_series)):
            # single dicom series level
            series = os.path.join(study, all_series[k])

            if os.path.isdir(series):

                os.chdir(series)
                dcm_files = sorted(next(os.walk("."))[2])

                # now search for tags
                for l in range(len(dcm_files)):
                    try:
                        ds = pydicom.dcmread(
                            dcm_files[l], stop_before_pixels=True, force=True
                        )
                    except:
                        print(dcm_files[l], ":is not a valid dicom")
                        pass

                    # here we need
                    # tags =

                    for tag in tags:
                        # change tags to dummy variable if they exist
                        if getattr(ds, tag, False):

                            try:
                                setattr(ds, tag, dummy)
                                # print('set:', tag, 'to:', dummy)
                            except:
                                print("unable to change:", tag, "for:", dcm_files[l])

                    if ".dcm" not in dcm_files[l]:
                        dcm_files[l] = dcm_files[l] + ".dcm"

                    dest_filepath = os.path.join(dest_path, dcm_files[l])
                    ds.save_as(dest_filepath)

    print("completed:", dcm_dir)


def insert_pixels():

    

    print("completed:", dcm_dir)



def count_tags():

    tag_count=0

    return tag_count

def count_pixels():

    pixel_count=0

    return pixel_count