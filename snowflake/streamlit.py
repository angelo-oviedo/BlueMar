import streamlit as st
from datetime import datetime
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Dashboard de Negocio para BlueMar", layout="wide")

# Título de la app
st.title("Dashboard de Negocio para BlueMar")
session = get_active_session()

# --- Barra Lateral ---
st.sidebar.header("Filtros")

# Seleccionar la tabla a analizar
tabla_seleccionada = st.sidebar.selectbox(
    "Seleccionar vista",
    ["Ingresos", "Gastos Operacionales", "Planillas", "Inventario de Activos"]
)

st.sidebar.subheader("Filtros Generales")
st.sidebar.selectbox("Seleccionar Año", ["2022", "2023", "2024"], index=1)
st.sidebar.multiselect("Seleccionar Meses", ["Enero", "Febrero", "Marzo", "Abril", "Mayo", 
                                             "Junio", "Julio", "Agosto", "Septiembre", 
                                             "Octubre", "Noviembre", "Diciembre"], default=["Enero", "Febrero"])
st.sidebar.multiselect("Seleccionar Clientes", ["Cliente A", "Cliente B", "Cliente C"], default=[])
st.sidebar.multiselect("Seleccionar Proveedores", ["Proveedor X", "Proveedor Y", "Proveedor Z"], default=[])
st.sidebar.slider("Rango de Valores", min_value=0, max_value=100000, value=(1000, 50000))

st.sidebar.button("Aplicar Filtros")

# --- Función para obtener datos ---
@st.cache_data
def obtener_ingresos_por_mes():
    query = """
        SELECT 
            TO_CHAR(fecha, 'YYYY-MM') AS mes, 
            SUM(total) AS ingresos_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_INGRESOS
        WHERE TO_CHAR(fecha, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY TO_CHAR(fecha, 'YYYY-MM')
        ORDER BY mes
    """
    return session.sql(query).to_pandas()

def obtener_promedio_ingresos_por_mes():
    query = """
        SELECT 
            TO_CHAR(fecha, 'YYYY-MM') AS mes, 
            AVG(total) AS promedio_ingresos
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_INGRESOS
        WHERE TO_CHAR(fecha, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY TO_CHAR(fecha, 'YYYY-MM')
        ORDER BY mes
    """
    return session.sql(query).to_pandas()

@st.cache_data
def obtener_gastos_por_mes():
    query = """
        SELECT 
            TO_CHAR(fecha_de_pago, 'YYYY-MM') AS mes, 
            SUM(total) AS gastos_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_GASTOS_OPERATIVOS
        WHERE TO_CHAR(fecha_de_pago, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY TO_CHAR(fecha_de_pago, 'YYYY-MM')
        ORDER BY mes
    """
    return session.sql(query).to_pandas()

@st.cache_data
def obtener_ingresos_por_cliente():
    query = """
        SELECT 
            c.nombre AS cliente,
            SUM(f.total) AS ingresos_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_INGRESOS f
        JOIN BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.DIM_CLIENTES c
            ON f.cliente_id = c.cliente_id
        WHERE TO_CHAR(f.fecha, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY c.nombre
        ORDER BY ingresos_totales DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data
def obtener_gastos_por_proveedor():
    query = """
        SELECT 
            p.nombre AS proveedor,
            SUM(fgo.total) AS gastos_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_GASTOS_OPERATIVOS fgo
        JOIN BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.DIM_PROVEEDORES p
            ON fgo.proveedor_id = p.proveedor_id
        WHERE TO_CHAR(fgo.fecha_de_pago, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY p.nombre
        ORDER BY gastos_totales DESC
    """
    return session.sql(query).to_pandas()

@st.cache_data
def obtener_gastos_por_top_proveedores():
    query = """
        SELECT 
            p.nombre AS proveedor,
            SUM(fgo.total) AS gastos_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_GASTOS_OPERATIVOS fgo
        LEFT JOIN BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.DIM_PROVEEDORES p
        ON fgo.proveedor_id = p.proveedor_id
        WHERE TO_CHAR(fgo.fecha_de_pago, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY p.nombre
        ORDER BY gastos_totales DESC
        LIMIT 10;
    """
    return session.sql(query).to_pandas()

@st.cache_data
def obtener_salarios_por_mes():
    query = """
        SELECT 
            TO_CHAR(FECHA_DEL_PAGO, 'YYYY-MM') AS mes, 
            SUM(PAGO_TOTAL) AS salarios_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_PLANILLAS
        WHERE TO_CHAR(FECHA_DEL_PAGO, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY TO_CHAR(FECHA_DEL_PAGO, 'YYYY-MM')
        ORDER BY mes;
    """
    return session.sql(query).to_pandas()


def obtener_salario_promedio_por_mes():
    query = """
        SELECT 
            TO_CHAR(FECHA_DEL_PAGO, 'YYYY-MM') AS mes, 
            AVG(PAGO_TOTAL) AS salario_promedio
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_PLANILLAS
        WHERE TO_CHAR(FECHA_DEL_PAGO, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY TO_CHAR(FECHA_DEL_PAGO, 'YYYY-MM')
        ORDER BY mes;
    """
    return session.sql(query).to_pandas()

def obtener_salarios_por_empleado():
    query = """
        SELECT 
            e.NOMBRE AS empleado,
            SUM(p.PAGO_TOTAL) AS salarios_totales
        FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_PLANILLAS p
        JOIN BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.DIM_EMPLEADOS e
        ON p.EMPLEADO_ID = e.EMPLEADO_ID
        WHERE TO_CHAR(p.FECHA_DEL_PAGO, 'YYYY') BETWEEN '2022' AND '2024'
        GROUP BY e.NOMBRE
        ORDER BY salarios_totales DESC;
    """
    return session.sql(query).to_pandas()

def obtener_valor_activos_por_tipo():
    query = """
        SELECT 
            tipo AS TIPO_ACTIVO,
            SUM(total) AS VALOR_TOTAL
        FROM BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_INVENTARIO_DE_ACTIVOS
        GROUP BY tipo
        ORDER BY VALOR_TOTAL DESC
    """
    return session.sql(query).to_pandas()

def obtener_cantidad_activos_por_anio():
    query = """
        SELECT 
            CAST(anho_de_adquisicion AS VARCHAR) AS anio_adquisicion, -- Cast para asegurar formato correcto
            SUM(cantidad) AS cantidad_total
        FROM BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_INVENTARIO_DE_ACTIVOS
        WHERE anho_de_adquisicion IS NOT NULL -- Eliminar nulos si es necesario
        GROUP BY anho_de_adquisicion
        ORDER BY anio_adquisicion
    """
    return session.sql(query).to_pandas()


def obtener_valor_activos_por_fuente():
    query = """
        SELECT 
            fuente_de_financiamiento AS fuente,
            SUM(total) AS valor_total
        FROM BLUEMAR_DEV_DBT_SCHEMA_MODELED.FACT_INVENTARIO_DE_ACTIVOS
        WHERE fuente_de_financiamiento IS NOT NULL -- Excluir nulos
        GROUP BY fuente_de_financiamiento
        ORDER BY valor_total DESC
    """
    return session.sql(query).to_pandas()


def obtener_top10_activos():
    query = """
        SELECT 
            nombre AS activo,
            total AS VALOR_TOTAL
        FROM BLUEMAR_DEV_DBT_SCHEMA_MODELED.DIM_ACTIVOS
        ORDER BY total DESC
        LIMIT 10
    """
    return session.sql(query).to_pandas()


# Validación y limpieza
def limpiar_mes(df):
    df["MES"] = pd.to_datetime(df["MES"], format="%Y-%m", errors="coerce")
    return df.dropna(subset=["MES"])

# Obtener datos
promedio_ingresos_por_mes = obtener_promedio_ingresos_por_mes()
ingresos = limpiar_mes(obtener_ingresos_por_mes())
gastos = limpiar_mes(obtener_gastos_por_mes())
ingresos_por_cliente = obtener_ingresos_por_cliente()
gastos_por_proveedor = obtener_gastos_por_proveedor()
gastos_por_top_proveedores = obtener_gastos_por_top_proveedores()
salarios_por_mes = obtener_salarios_por_mes()
salario_promedio_por_mes = obtener_salario_promedio_por_mes()
salarios_por_empleado = obtener_salarios_por_empleado()
valor_activos_por_tipo = obtener_valor_activos_por_tipo()
cantidad_activos_por_anio = obtener_cantidad_activos_por_anio()
valor_activos_por_fuente = obtener_valor_activos_por_fuente()
top10_activos = obtener_top10_activos()

if tabla_seleccionada == "Ingresos":
    st.subheader("Gráfica de Ingresos Totales por Mes")
    st.bar_chart(data=ingresos, x="MES", y="INGRESOS_TOTALES")
    
    st.subheader("Gráfica de Ingresos Totales por Cliente")
    st.bar_chart(data=ingresos_por_cliente, x="CLIENTE", y="INGRESOS_TOTALES")

    st.subheader("Promedio de Ingresos por Mes")
    st.line_chart(data=promedio_ingresos_por_mes, x="MES", y="PROMEDIO_INGRESOS")
    
elif tabla_seleccionada == "Gastos Operacionales":
    st.subheader("Gráfica de Gastos Totales por Proveedor")
    st.bar_chart(data=gastos_por_proveedor, x="PROVEEDOR", y="GASTOS_TOTALES")
    
    st.subheader("Gráfica de Gastos Totales por Principales Proveedores")
    st.bar_chart(data=gastos_por_top_proveedores, x="GASTOS_TOTALES", y="PROVEEDOR")
    
    st.subheader("Tendencia de Gastos Mensuales")
    st.line_chart(data=gastos, x="MES", y="GASTOS_TOTALES")

elif tabla_seleccionada == "Planillas":
    st.subheader("Gráfica de Salario Promedio por Mes")
    st.line_chart(data=salario_promedio_por_mes, x="MES", y="SALARIO_PROMEDIO")
    
    st.subheader("Gráfica de Salarios Totales por Mes")
    st.line_chart(data=salarios_por_mes, x="MES", y="SALARIOS_TOTALES")

    st.subheader("Distribución de Salarios por Empleado")
    st.bar_chart(data=salarios_por_empleado, x="EMPLEADO", y="SALARIOS_TOTALES")

elif tabla_seleccionada == "Inventario de Activos":
    st.subheader("Valor Total de Activos por Tipo")
    st.bar_chart(data=valor_activos_por_tipo, x="TIPO_ACTIVO", y="VALOR_TOTAL")
    
    st.subheader("Cantidad Total de Activos por Año de Adquisición")
    st.line_chart(data=cantidad_activos_por_anio, x="ANIO_ADQUISICION", y="CANTIDAD_TOTAL")
    
    st.subheader("Valor Total de Activos por Fuente de Financiamiento")
    st.bar_chart(data=valor_activos_por_fuente, x="FUENTE", y="VALOR_TOTAL")

    st.subheader("Top 10 Activos con Mayor Valor")
    st.bar_chart(data=top10_activos, x="ACTIVO", y="VALOR_TOTAL")
   
