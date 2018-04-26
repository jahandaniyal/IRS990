import csv

def filter_index_csv(input_file, output_file):

    index_csv = open(output_file, 'w', newline='')

    csv_writer = csv.writer(index_csv)

    with open(input_file, 'r') as csvfile:
        rb_ = csv.reader(csvfile, delimiter=',')
        for item in rb_:
            csv_writer.writerow([item[8]+'_public.xml', item[2]])
    index_csv.close()

def city_to_state(input_file):

    pass
    return 0

#filter_index_csv('index_2012.csv', 'test_file.csv')
