# importando os pacotse necessários para iniciar uma seção Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col, to_timestamp
from pyspark.sql.functions import year, month, date_format
import matplotlib.pyplot as plt

def main():

    # iniciando o spark context
    sc = SparkSession.builder.master('local[*]').getOrCreate()

    bo_acidentes_automobilisticos = (sc.read.format('csv')
                                     .option("ignoreLeadingWhiteSpace", "true")
                                     .option("ignoreTrailingWhiteSpace", "true")
                                     .option("encoding", "ISO-8859-1")
                                     .option("sep", ";")
                                     .option("inferSchema", "true")
                                     .option('header', 'True')
                                     .option("inferSchema", "true")
                                     .load('./si-bol-2019 (1).csv'))

    bo_acidentes_automobilisticos.createOrReplaceTempView("bo_acidentes_automobilisticos")
    bo_acidentes_automobilisticos = bo_acidentes_automobilisticos.drop("DATA_ALTERACAO_SMSA")

    bo_acidentes_automobilisticos.show()

    envolvidos_acidentes_automobilisticos = (sc.read.format('csv')
                                             .option("ignoreLeadingWhiteSpace", "true")
                                             .option("ignoreTrailingWhiteSpace", "true")
                                             .option("encoding", "ISO-8859-1")
                                             .option("sep", ";")
                                             .option("inferSchema", "true")
                                             .option('header', 'True')
                                             .option("inferSchema", "true")
                                             .load('./si_env-2019 (1).csv'))

    envolvidos_acidentes_automobilisticos.createOrReplaceTempView("envolvidos_acidentes_automobilisticos")

    envolvidos_acidentes_automobilisticos.show()

    localizacao_acidentes_automobilisticos = (sc.read.format('csv')
                                              .option("ignoreLeadingWhiteSpace", "true")
                                              .option("ignoreTrailingWhiteSpace", "true")
                                              .option("encoding", "ISO-8859-1")
                                              .option("sep", ";")
                                              .option("inferSchema", "true")
                                              .option('header', 'True')
                                              .option("inferSchema", "true")
                                              .load('./si-log-2019 (1).csv'))

    localizacao_acidentes_automobilisticos = localizacao_acidentes_automobilisticos.withColumnRenamed('Nº_boletim',
                                                                                                      'numero_boletim')
    localizacao_acidentes_automobilisticos.createOrReplaceTempView("localizacao_acidentes_automobilisticos")

    localizacao_acidentes_automobilisticos.show()

    join_acidentes = bo_acidentes_automobilisticos.join(
        envolvidos_acidentes_automobilisticos,
        bo_acidentes_automobilisticos.NUMERO_BOLETIM == envolvidos_acidentes_automobilisticos.num_boletim, "outer") \
        .join(
        localizacao_acidentes_automobilisticos,
        bo_acidentes_automobilisticos.NUMERO_BOLETIM == localizacao_acidentes_automobilisticos.numero_boletim, "outer")

    join_acidentes.show()

    join_acidentes.show()

    join_acidentes.na.drop(subset=["categoria_habilitacao", "pedestre", "passageiro"]) \
        .show(truncate=False)


    date = join_acidentes.select(
        to_date(col("DATA HORA_BOLETIM"), "dd/MM/yyyy HH:mm").alias("date_format")
    )

    date = date.groupBy(date_format("date_format", "MM/yyyy").alias("mes_ano")).count()
    date.show()

    data_collect = date.collect()
    mes_ano = [item["mes_ano"] for item in data_collect]
    count = [item["count"] for item in data_collect]

    plt.figure(figsize=(15, 3))
    plt.bar(mes_ano, count)
    plt.suptitle('Total de acidentes por mês/ano')

    df = join_acidentes.filter(col("DESC_TIPO_ACIDENTE").like("%COM VITIMA"))
    df_count = (df.groupBy("nome_bairro").count()
                .orderBy(col("count").desc())
                .limit(10)
                )


    acidente_por_bairro_collect = df_count.collect()
    nome_bairro = [item["nome_bairro"] for item in acidente_por_bairro_collect]
    count = [item["count"] for item in acidente_por_bairro_collect]

    plt.figure(figsize=(25, 3))
    plt.bar(nome_bairro, count)
    plt.suptitle('Top 10 bairros por total de acidentes com vítimas')

    plt.show()


if __name__ == '__main__':
    main()

