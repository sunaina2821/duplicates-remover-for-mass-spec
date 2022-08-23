import pandas as pd
import glob
import itertools
import os
import numpy as np


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

    for file in filenames:
        try:
            print(file)
            output_dictionary = {}

            df = pd.read_excel(
                file, sheet_name=None
            )  # making a dictionary where each key is related to the dataframe of each sheet

            merged_df = merge_pos_neg(df)  # dictionary

            merged_name = (
                "E:/new_data/merged/"
                + os.path.splitext(os.path.basename(file))[0]
                + "_merged.xlsx"
            )

            with pd.ExcelWriter(
                merged_name, engine="xlsxwriter"
            ) as writer:  # you can change the file path here to whatever you want
                for sheet, df in merged_df.items():
                    df.to_excel(writer, sheet_name=sheet)

            sheets = list(merged_df.keys())

            for sheet in sheets:
                per_sheet = merged_df[sheet]  # dataframe
                output = duplicate_analysis(per_sheet)  # dataframe
                output_dictionary[sheet] = output  # dictionary

            new_file_name = (
                "E:/new_data/output/"
                + os.path.splitext(os.path.basename(file))[0]
                + "_output.xlsx"
            )

            with pd.ExcelWriter(
                new_file_name, engine="xlsxwriter"
            ) as writer:  # you can change the file path here to whatever you want
                for sheet, merged_df in output_dictionary.items():
                    merged_df.to_excel(writer, sheet_name=sheet)
        except Exception as e:
            print("This file is giving an error ->" + str(file))
            print(repr(e))
            continue

        # TO DO: add a test for comparing the sum of 'n' and the total number of metabolites


def merge_pos_neg(dictionary):
    """
    Input as dictionary where each key is the sheet_name and value is the dataframe of that sheet's data.

    """
    keys = list(dictionary.keys())
    merged_diction = {}
    for a, b in itertools.combinations(keys, 2):
        split_a = list(a)  # split a into ['c', '1', 'p']
        split_b = list(b)  # split b into ['c', '1', 'n']
        if len(split_a) == 3 & len(split_b) == 3:
            if split_a[0] == split_b[0] and split_a[1] == split_b[1]:

                merged_df = pd.concat([dictionary[a], dictionary[b]], ignore_index=True)
                new_key = split_a[0] + split_a[1]
                merged_diction[new_key] = merged_df

        if len(split_a) == 4 & len(split_b) == 4:
            if (
                split_a[0] == split_b[0]
                and split_a[1] == split_b[1]
                and split_a[2] == split_b[2]
            ):

                merged_df = pd.concat([dictionary[a], dictionary[b]], ignore_index=True)
                new_key = split_a[0] + split_a[1] + split_a[2]
                merged_diction[new_key] = merged_df

    return merged_diction


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
    std_df = df.groupby("Metabolite Name")["Area"].agg({"std"}).reset_index()

    # making a new column z_score where all the rows are passed through the lambda function one by one and their z-scores are calculated
    df["z_score"] = df.groupby("Metabolite Name")["Area"].apply(
        lambda x: (x - x.mean()) / x.std()
    )

    # all the NAN values are changed to zero
    std_df["std"] = std_df["std"].fillna(0)

    df["z_score"] = df["z_score"].fillna(0)
    # all rows whose z-score is less than 1.25 are kept
    df = df[abs(df["z_score"]) < 1.25]
    # their mean is taken

    mean_df = (
        df.groupby("Metabolite Name")
        .agg(
            Area=("Area", "mean"),
            n=("Metabolite Name", "size"),
            z_score=("z_score", list),
        )
        .reset_index()
    )
    final_df = pd.concat([mean_df, std_df["std"]], axis=1)
    return final_df


if __name__ == "__main__":
    dir = "E:/LC-MS Data"  # add the path file here to the folder that contains all the data files
    data_cleaning(dir)
