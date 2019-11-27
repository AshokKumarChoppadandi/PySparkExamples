from pyspark import SparkContext, SparkConf


def get_employees_data():
    return [
        "Alice,10000,101",
        "Bob,20000,102",
        "Charlie,25000,101",
        "David,10000,102",
        "Gary,15000,101",
        "Henry,12000,103"
    ]


def calculate_average_salary():
    conf = SparkConf().setAppName("Average Salary for each department").setMaster("local")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    employees = get_employees_data()
    rdd = sc.parallelize(employees, 3)
    rdd2 = rdd.map(lambda rec: rec.split(','))
    rdd3 = rdd2.map(lambda x: (int(x[2]), float(x[1])))

    # Using groupByKey() transformation / operation
    group_rdd = rdd3.groupByKey()
    total_salary_employees1 = group_rdd.map(lambda x: (x[0], sum(x[1]), len(x[1])))
    average_salary1 = total_salary_employees1.map(lambda x: (x[0], x[1] / x[2]))
    result1 = average_salary1.collect()

    print('AVERAGE SALARY FOR EACH DEPARTMENT - USING GROUP BY KEY TRANSFORMATION')
    print('_' * 50)
    for (dept, avg_sal) in result1:
        print("%d department has an average salary of %f" % (dept, avg_sal))

    # Using reduceByKey() transformation / operation
    reduce_rdd1 = rdd3.reduceByKey(lambda x, y: x + y)
    reduce_rdd2 = rdd3.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)

    join_rdd = reduce_rdd1.join(reduce_rdd2)
    average_salary2 = join_rdd.map(lambda x: (x[0], x[1][0] / x[1][1]))
    result2 = average_salary2.collect()

    print('\nAVERAGE SALARY FOR EACH DEPARTMENT - USING REDUCE BY KEY TRANSFORMATION')
    print('_' * 50)
    for (dept, avg_sal) in result2:
        print("%d department has an average salary of %f" % (dept, avg_sal))

    # Using combineByKey() transformation / operation
    total_salary_employees2 = rdd3.combineByKey(
        lambda x: (x, 1),
        lambda x, y: (x[0] + y, x[1] + 1),
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )
    average_salary3 = total_salary_employees2.map(lambda x: (x[0], x[1][0] / x[1][1]))
    result3 = average_salary3.collect()

    print('\nAVERAGE SALARY FOR EACH DEPARTMENT - USING COMBINE BY KEY TRANSFORMATION')
    print('_' * 50)
    for (dept, avg_sal) in result3:
        print("%d department has an average salary of %f" % (dept, avg_sal))

    # Using aggregateByKey() transformation / operation
    total_salary_employees3 = rdd3.aggregateByKey(
        (0.0, 0),
        lambda x, y: (x[0] + y, x[1] + 1),
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )
    average_salary4 = total_salary_employees3.map(lambda x: (x[0], x[1][0] / x[1][1]))
    result4 = average_salary4.collect()
    print('\nAVERAGE SALARY FOR EACH DEPARTMENT - USING AGGREGATE BY KEY TRANSFORMATION')
    print('_' * 50)
    for (dept, avg_sal) in result4:
        print("%d department has an average salary of %f" % (dept, avg_sal))


def main():
    print('Executing by key Operations to Calculate the average salary for each department.')
    calculate_average_salary()
    print('Execution completed...!!!')


if __name__ == '__main__':
    main()
