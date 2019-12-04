import comparison

retrieve_nwis_out = "results/retrieve_nwis_{site_tag}_{beg_date}_{end_date}_{n}.out" 
retrieve_zarr_out = "results/retrieve_zarr_{site_tag}_{beg_date}_{end_date}.out" 
read_out = "results/read_{site_tag}_{beg_date}_{end_date}.out"
write_out = "results/write_{site_tag}_{beg_date}_{end_date}.out"

rule all:
    input:
        expand(retrieve_zarr_out, site_tag=['md'],
               beg_date=config['beg_date'], end_date=config['end_date']),

        expand(read_out, site_tag=['md'], beg_date=config['beg_date'],
              end_date=config['end_date']),

        expand(write_out, site_tag=['md'], beg_date=config['beg_date'],
              end_date=config['end_date']),
        # all retrieve_nwis
        #expand(retrieve_nwis_out, site_tag=['md'],
               #beg_date=config['beg_date'], end_date=config['end_date'],
               #n=config['n_per_chunk']),


        #expand(retrieve_nwis_out, site_tag=['lg'],
               #beg_date=config['beg_date'], end_date=config['end_date'],
               #n=['2']),



rule retrieve_zarr:
    output:
        retrieve_zarr_out
    run:
        comparison.time_retrieve_zarr(10, f"{wildcards.site_tag}_{wildcards.beg_date}_{wildcards.end_date}")


rule retrieve_nwis:
    output:
        retrieve_nwis_out
    run:
        comparison.time_retrieve_nwis(1, f"{wildcards.site_tag}_{wildcards.beg_date}_{wildcards.end_date}_{wildcards.n}")

rule time_write:  
    output:
        write_out
    run:
        print( "{wildcards.site_tag}_{wildcards.beg_date}_{wildcards.end_date}")
        comparison.time_write(10, f"{wildcards.site_tag}_{wildcards.beg_date}_{wildcards.end_date}")


rule time_read:  
    input:
        rules.time_write.output
    output:
        read_out
    run:
        comparison.time_read(10, f"{wildcards.site_tag}_{wildcards.beg_date}_{wildcards.end_date}")


