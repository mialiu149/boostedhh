#!/bin/bash

# make sure this is installed
# python3 -m pip install correctionlib==2.0.0rc6
# pip install --upgrade numpy==1.21.5

# make dir for output
mkdir outfiles

for t2_prefix in ${t2_prefixes}
do
    for folder in pickles parquet root jobchecks
    do
        xrdfs $${t2_prefix} mkdir -p -mrwxr-xr-x "/${outdir}/$${folder}"
    done
done

# try 3 times in case of network errors
(
    r=3
    # shallow clone of single branch (keep repo size as small as possible)
    while ! git clone --single-branch --recursive --branch $branch --depth=1 https://github.com/$gituser/$repo
    do
        ((--r)) || exit
        sleep 60
        rm -rf $repo  # remove folder in case it was made but something later failed.
        echo "Retrying git clone..."
        echo -e "\n\n\n"
    done
)
cd $repo || exit

commithash=$$(git rev-parse HEAD)
echo "https://github.com/$gituser/$repo/commit/$${commithash}" > commithash.txt

#move output to t2s
for t2_prefix in ${t2_prefixes}
do
    xrdcp -f commithash.txt $${t2_prefix}/${outdir}/jobchecks/commithash_${jobnum}.txt
done

pip install -e .
cd boostedhh
pip install -e .
cd ..

# run code
# pip install --user onnxruntime
python -u -W ignore $script --year $year --starti $starti --endi $endi --file-tag $filetag --samples $sample --subsamples $subsample --processor $processor --maxchunks $maxchunks --chunksize $chunksize ${save_root} ${save_systematics} --nano-version ${nano_version} $processor_args

#move output to t2s
for t2_prefix in ${t2_prefixes}
do
    xrdcp -f num_batches*.txt "$${t2_prefix}/${outdir}/jobchecks/"
    xrdcp -f outfiles/* "$${t2_prefix}/${outdir}/pickles/out_${jobnum}.pkl"
    xrdcp -f *.parquet "$${t2_prefix}/${outdir}/parquet/"
    xrdcp -f *.root "$${t2_prefix}/${outdir}/root/"
done

rm *.parquet
rm *.root
rm *.txt
