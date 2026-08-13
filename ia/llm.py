import os

from dotenv import load_dotenv
from openai import OpenAI

from ia.database import (
    get_schema,
    schema_to_text,
    validate_sql,
    execute_sql,
)

from ia.prompts import (
    SYSTEM_PROMPT,
    ANSWER_SYSTEM_PROMPT,
)


load_dotenv()


client = OpenAI(
    base_url=os.getenv("OLLAMA_URL"),
    api_key="ollama"
)


# ============================================================
# GENERACIÓN DE SQL
# ============================================================

def generate_sql(question: str) -> str:

    schema = get_schema()

    schema_text = schema_to_text(schema)

    system_prompt = SYSTEM_PROMPT.format(
        schema=schema_text
    )

    response = client.chat.completions.create(
        model=os.getenv("OLLAMA_MODEL"),

        messages=[
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": question
            }
        ],

        temperature=0
    )

    sql = response.choices[0].message.content.strip()

    # Limpiar posibles bloques Markdown
    if sql.startswith("```sql"):
        sql = sql[6:]

    if sql.startswith("```"):
        sql = sql[3:]

    if sql.endswith("```"):
        sql = sql[:-3]

    return sql.strip()


# ============================================================
# GENERACIÓN DE RESPUESTA
# ============================================================

def generate_answer(question: str, results):

    answer_prompt = ANSWER_SYSTEM_PROMPT.format(
    question=question,
    results=results
    )

    response = client.chat.completions.create(
        model=os.getenv("OLLAMA_MODEL"),

        messages=[
            {
                "role": "system",
                "content": answer_prompt
            }
        ],

        temperature=0
    )

    answer = response.choices[0].message.content.strip()

    prefixes = [
        "assistant",
        "Assistant:",
        "assistant:",
    ]

    for prefix in prefixes:
        if answer.startswith(prefix):
            answer = answer[len(prefix):].strip()

    return answer


# ============================================================
# CICLO COMPLETO
# ============================================================

def ask_database(question: str):

    # ---------------------------------------------------------
    # 1. GENERAR SQL
    # ---------------------------------------------------------

    sql = generate_sql(question)

    # ---------------------------------------------------------
    # 2. VALIDAR SQL
    # ---------------------------------------------------------

    is_valid, error = validate_sql(sql)

    if not is_valid:

        return {
            "answer": None,
            "sql": sql,
            "error": error,
            "results": None,
        }

    # ---------------------------------------------------------
    # 3. EJECUTAR SQL
    # ---------------------------------------------------------

    try:

        results = execute_sql(sql)

    except Exception as e:

        return {
            "answer": None,
            "sql": sql,
            "error": str(e),
            "results": None,
        }

    # ---------------------------------------------------------
    # 4. GENERAR RESPUESTA NATURAL
    # ---------------------------------------------------------

    try:

        answer = generate_answer(
            question,
            results
        )

    except Exception as e:

        return {
            "answer": None,
            "sql": sql,
            "error": (
                "Los datos se obtuvieron correctamente, "
                f"pero no se pudo generar la respuesta: {str(e)}"
            ),
            "results": results,
        }

    # ---------------------------------------------------------
    # 5. DEVOLVER RESULTADO FINAL
    # ---------------------------------------------------------

    return {
        "answer": answer,
        "sql": sql,
        "error": None,
        "results": results,
    }