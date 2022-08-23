import pandas as pd
import glob
import itertools
import os
import numpy as np
import traceback


def data_cleaning(dir1, dir2):  # input as a folder

    """
    This method runs a loop through two directories, selecting their files and then processing them through
    merged() and duplicate_analysis().

    Parameters :
    - dir1 : directory of the folder named 'POSITIVE' containing all the positive tests
    - dir2 : directory of the folder named 'NEGATIVE' containing all the negative tests

    Returns :
    - two excel files saved in a new folder named output and merged

    Author : Sunaina
    """

    file_1 = glob.glob(dir1 + "\*.xlsx")  # pos
    file_2 = glob.glob(dir2 + "\*.xlsx")  # neg

    filenames = []

    # filenames = filenames.append(file_1) # ["POS.xlsx", "NEG.xlsx"]
    for file1, file2 in zip(file_1, file_2):
        try:
            positive = file1
            negative = file2

            output_dictionary = {}

            df1 = pd.read_excel(
                positive, sheet_name=None
            )  # making a dictionary where each key is related to the dataframe of each sheet

            df2 = pd.read_excel(negative, sheet_name=None)

            merged_df = merge_pos_neg(df1, df2)  # dictionary

            merged_name = (
                "E:/new_data/merged/"
                + os.path.splitext(os.path.basename(file1))[0]
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
                + os.path.splitext(os.path.basename(file1))[0]
                + "_output.xlsx"
            )

            with pd.ExcelWriter(
                new_file_name, engine="xlsxwriter"
            ) as writer:  # you can change the file path here to whatever you want
                for sheet, merged_df in output_dictionary.items():
                    merged_df.to_excel(writer, sheet_name=sheet)

        except Exception as e:
            print("This file is giving an error ->" + str(file1) + str(file2))
            print(traceback.format_exc())
            continue

        # TO DO: add a test for comparing the sum of 'n' and the total number of metabolites


def merge_pos_neg(dic_1, dic_2):

    """
    Input as two dictionaries where each key is the sheet_name and value is the dataframe of that sheet's data.

    """
    keys_1 = list(dic_1.keys())  # ['SAM01', 'SAM02']
    keys_2 = list(dic_2.keys())  # ['SAM01', 'SAM02']

    merged_diction = {}

    # for a, b in zip(keys_1, keys_2): # for i in [('SAM01', 'SAM02')]

    for i, j in zip(keys_1, keys_2):
        if i == j:
            merged_df = pd.concat([dic_1[i], dic_2[j]], ignore_index=True)
            merged_diction[i] = merged_df

            # merged_df = pd.concat([dictionary[a], dictionary[b]], ignore_index=True)
            # new_key = split_a[0] + split_a[1] + split_a[2]
            # merged_diction[new_key] = merged_df

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

    df = df.sort_values("metabolite name")
    std_df = df.groupby("metabolite name")["Area"].agg({"std"}).reset_index()

    # making a new column z_score where all the rows are passed through the lambda function one by one and their z-scores are calculated
    df["z_score"] = df.groupby("metabolite name")["Area"].apply(
        lambda x: (x - x.mean()) / x.std()
    )

    # all the NAN values are changed to zero
    std_df["std"] = std_df["std"].fillna(0)

    df["z_score"] = df["z_score"].fillna(0)
    # all rows whose z-score is less than 1.25 are kept
    df = df[abs(df["z_score"]) < 1.25]
    # their mean is taken

    mean_df = (
        df.groupby("metabolite name")
        .agg(
            Area=("Area", "mean"),
            n=("metabolite name", "size"),
            z_score=("z_score", list),
        )
        .reset_index()
    )

    final_df = pd.concat([mean_df, std_df["std"]], axis=1)
    return final_df


# two folders ->

if __name__ == "__main__":
    dir1 = "E:/data_/POSITIVE"  # add the path file here to the folder that contains all the data files
    dir2 = "E:/data_/NEGATIVE"
    data_cleaning(dir1, dir2)
