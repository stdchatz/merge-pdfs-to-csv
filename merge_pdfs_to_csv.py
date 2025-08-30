import pandas as pd
import camelot
import glob
import os

# folder with pdfs
input_folder = "./pdfs"
pdf_files = glob.glob(os.path.join(input_folder, "*.pdf"))

all_dfs = []

for pdf_file in pdf_files:
    print(f"Processing {pdf_file} ...")

    # using stream as no clear borders
    tables = camelot.read_pdf(pdf_file, pages="all", flavor="stream", strip_text="\n")

    # merge tables from this pdf 
    df_list = [t.df for t in tables]
    df = pd.concat(df_list, ignore_index=True)

    # remove empty rows
    df = df.dropna(how="all")

    # force consistent schema
    df.columns = ["City", "Full Amount", "Max Amount (w/utilities)",
                  "Max Amount (only rent)", "No (manual)", "Fixed Amount",
                  "Not Yet", "Total", "Percentage"]

    # remove rows that are header repeats
    df = df[df["City"] != "Full Amount"]

    # add file name to track source
    df["Source_File"] = os.path.basename(pdf_file)

    # move Source_File to the first column
    cols = ["Source_File"] + [col for col in df.columns if col != "Source_File"]
    df = df[cols]

    all_dfs.append(df)

# merge all pdfs
merged_df = pd.concat(all_dfs, ignore_index=True)

# save
merged_df.to_csv("merged_output.csv", index=False)
merged_df.to_excel("merged_output.xlsx", index=False)

print("PDFs processed. Files saved as merged_output.csv and merged_output.xlsx")