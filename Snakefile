outfile_format = "results/{site_bunch}_{beg_date}_{end_date}_{n}.out" 
rule all:
    input:
        # all med
        expand(outfile_format, site_bunch=['md'], beg_date=config['beg_date'],
              end_date=config['end_date'], n=config['n_per_chunk']),
        # just one lg
        expand(outfile_format, site_bunch=['lg'], beg_date=config['beg_date'],
              end_date=config['end_date'], n=[2]),

rule run_timings:  
    output:
        outfile_format
    shell:
        "python comparison.py -b {wildcards.beg_date} -e {wildcards.end_date}"
        " -s {wildcards.site_bunch} -n {wildcards.n} > {output}" 
        
