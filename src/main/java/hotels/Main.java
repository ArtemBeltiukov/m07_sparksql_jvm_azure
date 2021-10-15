package hotels;

import hotels.services.PropertiesService;
import hotels.services.SparkService;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {

        final PropertiesService PROPERTIES = new PropertiesService();
        SparkService sparkService = new SparkService();
        SparkSession ss = sparkService.sparkSession();
        Dataset<Row> expedia = ss.read().format("com.databricks.spark.avro").load("abfss://m07sparksql@bd201stacc.dfs.core.windows.net/expedia");
        expedia.filter((FilterFunction<Row>) x -> {
            return x.get(x.fieldIndex("srch_ci")) != null
                    && x.get(x.fieldIndex("srch_co")) != null
                    && x.getDate(x.fieldIndex("srch_ci")).before(x.getDate(x.fieldIndex("srch_co")));
        });
        Dataset<Row> hotels = ss.read().format("parquet").load("abfss://m07sparksql@bd201stacc.dfs.core.windows.net/hotel-weather");

        hotels.write().format("delta").mode(SaveMode.Overwrite).save("/mnt/hotel_weather");
        expedia.write().format("delta").mode(SaveMode.Overwrite).save("/mnt/expedia");

        ss.sql("drop table if exists expedia;");
        ss.sql("drop table if exists hotel_weather;");
        ss.sql("create table expedia using delta location \"/mnt/expedia\"");
        ss.sql("create table hotel_weather using delta location \"/mnt/hotel_weather\"");

        Dataset<Row> firstQuery = ss.sql("select id, address, max(diff) as diff\n" +
                "from(\n" +
                "select max(avg_tmpr_c) - min(avg_tmpr_c) as diff,\n" +
                "id, address\n" +
                "from hotel_weather\n" +
                "group by month, id, address\n" +
                ")\n" +
                "group by id, address\n" +
                "order by diff desc\n" +
                "limit 10");
        firstQuery.show();

        Dataset<Row> secondQuery = ss.sql("with dates as (select month(srch_ci) as month, year(srch_ci) as year\n" +
                "from expedia\n" +
                "where srch_ci is not null\n" +
                "and srch_co is not null\n" +
                "and srch_ci <= srch_co\n" +
                "union distinct\n" +
                "select month(srch_co) as month, year(srch_co) as year\n" +
                "from expedia\n" +
                "where srch_ci is not null\n" +
                "and srch_co is not null\n" +
                "and srch_ci <= srch_co\n" +
                "group by month(srch_ci), year(srch_ci) , month(srch_co), year(srch_co)\n" +
                "order by year, month)\n" +
                "\n" +
                "select * \n" +
                "from(\n" +
                "select date(concat(year, \"-\", month,\"-\",1)) as date, hotel_id, count(user_id) as count,  Rank() \n" +
                "          over (partition by date(concat(year, \"-\", month,\"-\",1))\n" +
                "          ORDER BY count(user_id) DESC, hotel_id ) AS Rank\n" +
                "from dates\n" +
                "left join expedia on\n" +
                "date(concat(year, \"-\", month,\"-\",1)) between srch_ci and srch_co\n" +
                "or (month(srch_ci) =  month(date(concat(year, \"-\", month,\"-\",1))) and year(srch_ci) =  year(date(concat(year, \"-\", month,\"-\",1))))\n" +
                "or (month(srch_co) =  month(date(concat(year, \"-\", month,\"-\",1))) and year(srch_co) =  year(date(concat(year, \"-\", month,\"-\",1))))\n" +
                "\n" +
                "group by date,year,month, hotel_id)\n" +
                "\n" +
                "where rank <=10\n" +
                "group by date, hotel_id, count, rank\n" +
                "order by date, count desc, rank");
        secondQuery.show();

        Dataset<Row> thirdQuery = ss.sql("select srch_ci, srch_co, user_id, hotel_id,\n" +
                "checkout_wthr.avg_tmpr_c - checkin_wthr.avg_tmpr_c as t_diff,\n" +
                "avg(avg_wthr.avg_tmpr_c) as average_t\n" +
                "from expedia\n" +
                "\n" +
                "left join hotel_weather as checkin_wthr\n" +
                "on hotel_id = checkin_wthr.id\n" +
                "and srch_ci = checkin_wthr.wthr_date\n" +
                "\n" +
                "left join hotel_weather as checkout_wthr\n" +
                "on hotel_id = checkout_wthr.id\n" +
                "and srch_co = checkout_wthr.wthr_date\n" +
                "\n" +
                "left join hotel_weather as avg_wthr\n" +
                "on hotel_id = avg_wthr.id\n" +
                "and avg_wthr.wthr_date between srch_ci and srch_co\n" +
                "\n" +
                "where srch_ci is not null\n" +
                "and srch_co is not null\n" +
                "and srch_ci <= srch_co\n" +
                "and datediff(srch_co, srch_ci) > 7\n" +
                "and checkin_wthr.avg_tmpr_c is not null\n" +
                "and checkout_wthr.avg_tmpr_c is not null\n" +
                "\n" +
                "group by srch_ci, srch_co, user_id, hotel_id, checkin_wthr.avg_tmpr_c, checkout_wthr.avg_tmpr_c");

        thirdQuery.show();

        thirdQuery.write().format("parquet").mode(SaveMode.Overwrite).save(PROPERTIES.getProperty("azure.saveFolder")+"/thirdQuery");
        secondQuery.write().format("parquet").mode(SaveMode.Overwrite).save(PROPERTIES.getProperty("azure.saveFolder")+"/secondQuery");
        firstQuery.write().format("parquet").mode(SaveMode.Overwrite).save(PROPERTIES.getProperty("azure.saveFolder")+"/firstQuery");
    }
}
