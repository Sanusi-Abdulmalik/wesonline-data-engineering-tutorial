import sys
import pandas as pd
import loader

def load_bronze():
    try:
        print('started processing bronze_layer')
        df = pd.read_excel(r"/opt/airflow/my_files/Survey Data.xlsx")

        print(df.head(3))

        loader.write_to_db(data_df=df, table_name='survey_data'
                        ,mode='replace',schema='bronze_layer')
        print("Done processing bronze_data")

    except Exception as err:
        print(f'failed processing bronze data with Error : {err}')
        raise


def load_silver_data():
    try:
        print("Started processing silver data")
        query = """SELECT CAST("Timestamp" AS DATE) AS survey_date,
        "How old are you?" AS age_range,
        "What industry do you work in?" AS industry,
        "Job title" AS job_title,
        "If your job title needs additional context, please clarify here"  AS job_title_context,
        "What is your annual salary? (You'll indicate the currency in a "  AS annual_salary,
        "How much additional monetary compensation do you get, if any (f"   AS additional_compensation,
        "Please indicate the currency"  AS currency,
        'If "Other," please indicate the currency here:'  AS other_currency,
        "If your income needs additional context, please provide it here"  AS income_context,
        "What country do you work in?"  AS country,
        "If you're in the U.S., what state do you work in?"  AS us_state,
        "What city do you work in?"  AS city,
        "How many years of professional work experience do you have over"  
        AS total_years_experience,
        "How many years of professional work experience do you have in y"  
        AS field_years_experience,
        "What is your highest level of education completed?" AS education_level,
        "What is your gender?" AS gender,
        "What is your race? (Choose all that apply.)" AS race FROM bronze_layer.survey_data"""

        df = loader.read_df(query=query)

        print("Successfully processed bronze_data from DB")

        print(df.head(3))

        loader.write_to_db(data_df=df, table_name='survey_data_transformed'
                        ,mode='replace',schema='silver_layer')
        
        print("Done processing silver data")
    except Exception as err:
        print(f'Failed processing silver data with Error : {err}')

def load_gold_data():
    try:
        print("Started processing gold data")

        query = """
        SELECT
            industry,
            country,
            gender,
            education_level,
            annual_salary
        FROM silver_layer.survey_data_transformed
        """

        df = loader.read_df(query=query)

        print("Successfully loaded silver data")

        # Convert salary to numeric
        df["annual_salary"] = pd.to_numeric(df["annual_salary"], errors="coerce")

        # Remove null salaries
        df = df.dropna(subset=["annual_salary"])

        # ==========================
        # Gold Table 1
        # Average Salary by Industry
        # ==========================

        industry_salary = (
            df.groupby("industry", as_index=False)
              .agg(
                  average_salary=("annual_salary", "mean"),
                  employee_count=("annual_salary", "count")
              )
        )

        loader.write_to_db(
            data_df=industry_salary,
            table_name="industry_salary_summary",
            mode="replace",
            schema="gold_layer"
        )

        # ==========================
        # Gold Table 2
        # Salary by Country
        # ==========================

        country_salary = (
            df.groupby("country", as_index=False)
              .agg(
                  average_salary=("annual_salary", "mean"),
                  employee_count=("annual_salary", "count")
              )
        )

        loader.write_to_db(
            data_df=country_salary,
            table_name="country_salary_summary",
            mode="replace",
            schema="gold_layer"
        )

        # ==========================
        # Gold Table 3
        # Salary by Gender
        # ==========================

        gender_salary = (
            df.groupby("gender", as_index=False)
              .agg(
                  average_salary=("annual_salary", "mean"),
                  employee_count=("annual_salary", "count")
              )
        )

        loader.write_to_db(
            data_df=gender_salary,
            table_name="gender_salary_summary",
            mode="replace",
            schema="gold_layer"
        )

        print("Done processing gold data")

    except Exception as err:
        print(f"Failed processing gold data with Error: {err}")
        raise

if __name__ == '__main__':
    run_module = sys.argv[1]
    if run_module == '1':
        load_bronze()
    elif run_module == '2':
        load_silver_data()
    elif run_module == '3':
        load_gold_data()