from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional


app = FastAPI()

# Allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "gen-lang-client-0413676114"
DATASET = "property_mgmt"

# IMPORTANT:
# If your table is named "expense" in BigQuery, keep this as "expense".
# If your table is named "expenses", change it below.
EXPENSE_TABLE = "expense"


# -------------------------------------------------------------------
# BigQuery client dependency
# -------------------------------------------------------------------
def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# -------------------------------------------------------------------
# Request body models
# -------------------------------------------------------------------
class IncomeCreate(BaseModel):
    income_id: int
    amount: float
    date: str
    description: Optional[str] = None


class ExpenseCreate(BaseModel):
    expense_id: int
    amount: float
    date: str
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None


# -------------------------------------------------------------------
# Root
# -------------------------------------------------------------------
@app.get("/")
def read_root():
    return {"message": "Property Management API is running"}


# -------------------------------------------------------------------
# Properties
# -------------------------------------------------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
        properties = [dict(row) for row in results]
        return properties
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/properties/{property_id}")
def get_property_by_id(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())

        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        return dict(results[0])

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


# -------------------------------------------------------------------
# Income
# -------------------------------------------------------------------
@app.get("/income/{property_id}")
def get_income_by_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    income_query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_results = list(bq.query(check_query, job_config=job_config).result())
        if not property_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        results = bq.query(income_query, job_config=job_config).result()
        income_records = [dict(row) for row in results]
        return income_records

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/income/{property_id}")
def create_income_record(
    property_id: int,
    income: IncomeCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income`
        (income_id, property_id, amount, date, description)
        VALUES
        (@income_id, @property_id, @amount, @date, @description)
    """

    insert_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("income_id", "INT64", income.income_id),
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", income.amount),
            bigquery.ScalarQueryParameter("date", "DATE", income.date),
            bigquery.ScalarQueryParameter("description", "STRING", income.description),
        ]
    )

    try:
        property_results = list(bq.query(check_query, job_config=check_config).result())
        if not property_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        bq.query(insert_query, job_config=insert_config).result()

        return {
            "message": "Income record created successfully",
            "property_id": property_id,
            "income_id": income.income_id
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Insert failed: {str(e)}"
        )


# -------------------------------------------------------------------
# Expenses
# -------------------------------------------------------------------
@app.get("/expenses/{property_id}")
def get_expenses_by_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    expense_query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.{EXPENSE_TABLE}`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_results = list(bq.query(check_query, job_config=job_config).result())
        if not property_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        results = bq.query(expense_query, job_config=job_config).result()
        expense_records = [dict(row) for row in results]
        return expense_records

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/expenses/{property_id}")
def create_expense_record(
    property_id: int,
    expense: ExpenseCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.{EXPENSE_TABLE}`
        (expense_id, property_id, amount, date, category, vendor, description)
        VALUES
        (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
    """

    insert_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expense_id", "INT64", expense.expense_id),
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", expense.amount),
            bigquery.ScalarQueryParameter("date", "DATE", expense.date),
            bigquery.ScalarQueryParameter("category", "STRING", expense.category),
            bigquery.ScalarQueryParameter("vendor", "STRING", expense.vendor),
            bigquery.ScalarQueryParameter("description", "STRING", expense.description),
        ]
    )

    try:
        property_results = list(bq.query(check_query, job_config=check_config).result())
        if not property_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        bq.query(insert_query, job_config=insert_config).result()

        return {
            "message": "Expense record created successfully",
            "property_id": property_id,
            "expense_id": expense.expense_id
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Insert failed: {str(e)}"
        )