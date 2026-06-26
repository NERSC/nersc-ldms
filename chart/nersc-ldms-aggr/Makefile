.PHONY: all clean

# Check if a file named "ldms_machine_config.json" exists
ifeq (,$(wildcard ldms_machine_config.json))
    $(error ldms_machine_config.json is missing! Aborting. Create file.)
endif

# Main target
all: manifest.yaml

# Define products to be cleaned
products = out_dir \
           nersc-ldms-aggr/templates/cm.nersc-ldms-bin.yaml \
           nersc-ldms-aggr/templates/cm.nersc-ldms-conf.yaml \
           values.yaml \
           manifest.yaml

# Declare generated files depend on a common marker
out_dir/cm.nersc-ldms-bin.yaml: out_dir/.generated

out_dir/cm.nersc-ldms-conf.yaml: out_dir/.generated

out_dir/nersc-ldmsd-port-map.json: out_dir/.generated

# Marker target to run the init scripts only once
# Host map provider is determined by sys_opts.host_map_provider in ldms_machine_config.json
# Default: dell. Set to "csm" for CSM-based host map generation.
out_dir/.generated:
	@HOST_MAP_PROVIDER=$$(python3 -c "import json; c=json.load(open('ldms_machine_config.json')); print(c.get('sys_opts',{}).get('host_map_provider','dell'))" 2>/dev/null || echo "dell"); \
	if [ "$$HOST_MAP_PROVIDER" = "csm" ]; then \
		echo "Running make_host_map.csm.py (host_map_provider=csm)"; \
		./make_host_map.csm.py --config=ldms_machine_config.json; \
	else \
		echo "Running ./make_host_map.dell.py (host_map_provider=dell)"; \
		./make_host_map.dell.py --config=ldms_machine_config.json; \
	fi
	echo "Running ./nersc_ldms_make_ldms_config.py --config=ldms_machine_config.json"; \
	./nersc_ldms_make_ldms_config.py --config=ldms_machine_config.json
	touch $@

# Copy templates from generated files
nersc-ldms-aggr/templates/cm.nersc-ldms-bin.yaml: out_dir/cm.nersc-ldms-bin.yaml
	cp -f $< $@

nersc-ldms-aggr/templates/cm.nersc-ldms-conf.yaml: out_dir/cm.nersc-ldms-conf.yaml
	cp -f $< $@

# Generate manifest.yaml from the template
manifest.yaml: manifest.yaml.in out_dir/cm.nersc-ldms-conf.yaml out_dir/cm.nersc-ldms-bin.yaml
	./mkmanifest.py > manifest.yaml

# Clean up generated files
clean:
	rm -rf $(products)
