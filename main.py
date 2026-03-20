from oracle.Job import Job

def main():
    job=Job(source_path='~/Downloads/olympus_test.csv')
    job.run_job()

if __name__ == '__main__':
    main()