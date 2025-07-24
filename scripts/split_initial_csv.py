import pandas as pd
import os

INITIAL_CSV_FILE_PATH = "../initial_data/database.csv"
DESTINATION_FOLDER = "../testing_data"
CHUNK_SIZE = 10000

def main():
    os.makedirs(DESTINATION_FOLDER, exist_ok=True)
    
    batched_df = pd.read_csv(INITIAL_CSV_FILE_PATH, chunksize=CHUNK_SIZE)

    for batch in batched_df:
        if "departure" not in batch.columns:
            print("Departure date column is not present")
            return
        
        batch["departure"] = pd.to_datetime(batch["departure"], errors='coerce')
        batch = batch[batch["departure"].notna()]
        
        batch["year_month"] = batch["departure"].dt.strftime('%Y-%m')
        
        for year_month, group in batch.groupby("year_month"):
            output_file_path = os.path.join(DESTINATION_FOLDER, f"{year_month}.csv")
            group.drop(columns=['year_month']).to_csv(
                output_file_path, 
                mode="a",
                header = not os.path.exists(output_file_path),
                index=False
            )
         

if __name__ == "__main__":
    main()