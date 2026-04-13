from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional

class PropertyInput(BaseModel):
    name: Optional[str] = None
    address: str
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float

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
EXPENSE_TABLE = "expense"


# -------------------------------------------------------------------
# BigQuery client dependency
# -------------------------------------------------------------------
def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
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
# Expense
# -------------------------------------------------------------------
@app.get("/expense/{property_id}")
def get_expense_by_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
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


@app.post("/expense/{property_id}")
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


# -------------------------------------------------------------------
# Additional Endpoints
# -------------------------------------------------------------------

# 1. Total Income
@app.get("/properties/{property_id}/total-income")
def get_total_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    total_income_query = f"""
        SELECT SUM(amount) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
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

        results = list(bq.query(total_income_query, job_config=job_config).result())
        total_income = results[0]["total_income"] if results else 0

        return {
            "property_id": property_id,
            "total_income": total_income if total_income is not None else 0
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# 2. Total Expense
@app.get("/properties/{property_id}/total-expense")
def get_total_expense(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    expense_query = f"""
        SELECT SUM(amount) AS total_expense
        FROM `{PROJECT_ID}.{DATASET}.{EXPENSE_TABLE}`
        WHERE property_id = @property_id
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

        results = list(bq.query(expense_query, job_config=job_config).result())
        total_expense = results[0]["total_expense"] if results else 0

        return {
            "property_id": property_id,
            "total_expense": total_expense if total_expense is not None else 0
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# 3. Net Profit
@app.get("/properties/{property_id}/net-profit")
def get_net_profit(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    income_query = f"""
        SELECT SUM(amount) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
    """

    expense_query = f"""
        SELECT SUM(amount) AS total_expense
        FROM `{PROJECT_ID}.{DATASET}.{EXPENSE_TABLE}`
        WHERE property_id = @property_id
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

        income_result = list(bq.query(income_query, job_config=job_config).result())
        expense_result = list(bq.query(expense_query, job_config=job_config).result())

        total_income = income_result[0]["total_income"] if income_result else 0
        total_expense = expense_result[0]["total_expense"] if expense_result else 0

        total_income = total_income if total_income is not None else 0
        total_expense = total_expense if total_expense is not None else 0

        return {
            "property_id": property_id,
            "total_income": total_income,
            "total_expense": total_expense,
            "net_profit": total_income - total_expense
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# 4. Average Expense
@app.get("/properties/{property_id}/average-expense")
def get_average_expense(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):

    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    avg_query = f"""
        SELECT AVG(amount) AS avg_expense
        FROM `{PROJECT_ID}.{DATASET}.{EXPENSE_TABLE}`
        WHERE property_id = @property_id
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

        results = list(bq.query(avg_query, job_config=job_config).result())
        avg_expense = results[0]["avg_expense"] if results else 0

        return {
            "property_id": property_id,
            "average_expense": avg_expense if avg_expense is not None else 0
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

#add
@app.post("/properties")
def create_property(property_data: PropertyInput):
    client = bigquery.Client()

    query = """
    INSERT INTO `gen-lang-client-0413676114.property_mgmt.properties`
    (name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
    VALUES
    (@name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
            bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
            bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
            bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
        ]
    )

    client.query(query, job_config=job_config).result()
    return {"message": "Property created successfully"}


#add
@app.put("/properties/{property_id}")
def update_property(property_id: int, property_data: PropertyInput):
    client = bigquery.Client()

    query = """
    UPDATE `gen-lang-client-0413676114.property_mgmt.properties`
    SET
        name = @name,
        address = @address,
        city = @city,
        state = @state,
        postal_code = @postal_code,
        property_type = @property_type,
        tenant_name = @tenant_name,
        monthly_rent = @monthly_rent
    WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
            bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
            bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
            bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
        ]
    )

    client.query(query, job_config=job_config).result()
    return {"message": "Property updated successfully"}

@app.delete("/properties/{property_id}")
def delete_property(property_id: int):
    client = bigquery.Client()

    delete_income_query = """
    DELETE FROM `gen-lang-client-0413676114.property_mgmt.income`
    WHERE property_id = @property_id
    """

    delete_expense_query = """
    DELETE FROM `gen-lang-client-0413676114.property_mgmt.expense`
    WHERE property_id = @property_id
    """

    delete_property_query = """
    DELETE FROM `gen-lang-client-0413676114.property_mgmt.properties`
    WHERE property_id = @property_id
    """

    income_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    expense_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    property_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    client.query(delete_income_query, job_config=income_config).result()
    client.query(delete_expense_query, job_config=expense_config).result()
    client.query(delete_property_query, job_config=property_config).result()

    return {"message": "Property and related records deleted successfully"}

