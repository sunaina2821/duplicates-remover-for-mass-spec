import pandas as pd
import glob


def data_cleaning(dir):  # input as a folder
    """
    This method runs a loop through all the .xlsx files in a folder, converts each sheet of the excel file to a dataframe, passes those sheets
    through another method named duplicate_analysis which removes all the duplicates.

    Parameters :
    - dir : directory of the folder

    Returns :
    - excel file saved in the same directory as this file but its path can be changed

    Author : Sunaina
    """
    filenames = glob.glob(dir + "\*.xlsx")  #
    dictionary = {}
    for file in filenames:
        df = pd.read_excel(
            file, sheet_name=None
        )  # making a dictionary where each key is related to the dataframe of each sheet
        keys = list(df.keys())
        for key in keys:
            per_sheet = df[key]
            output = duplicate_analysis(per_sheet)
            dictionary[key] = output

        with pd.ExcelWriter(
            "output.xlsx", engine="xlsxwriter"
        ) as writer:  # you can change the file path here to whatever you want
            for sheet, df in dictionary.items():
                df.to_excel(writer, sheet_name=sheet)


def duplicate_analysis(per_sheet):  # of each sheet
    """
    This method takes in a dataframe and removes duplicates from it that are 1.25 standard deviations away from the mean.

    Parameters :
    - per_sheet : dataframe of each sheet of the excel file

    Returns :
    - excel file saved in the same directory as this file but its path can be changed

    Author : Sunaina
    """
    df = per_sheet

    df = df.sort_values("Metabolite Name")
    # making a new column z_score where all the rows are passed through the lambda function one by one and their z-scores are calculated
    df["z_score"] = df.groupby("Metabolite Name")["Area"].apply(
        lambda x: (x - x.mean()) / x.std()
    )
    # all the NAN values are changed to zero
    df["z_score"] = df["z_score"].fillna(0)
    # all rows whose z-score is less than 1.25 are kept
    df = df[abs(df["z_score"]) < 1.25]
    # their mean is taken
    mean_df = (
        df.groupby("Metabolite Name")
        .agg(Area=("Area", "mean"), n=("Metabolite Name", "size"))
        .reset_index()
    )
    return mean_df


if __name__ == "__main__":
    dir = "C:/Users/Lenovo/Spec_data"  # add the path file here to the folder that contains all the data files
    data_cleaning(dir)
