#!/bin/bash
#PBS -A train_bigdat16
#PBS -l walltime=00:05:00
#PBS -l select=1:ncpus=16:mem=64GB
#PBS -q parallel
#PBS -p 1020
module load autoload jre/1.8.0_73
current_folder=`pwd`/bd

#SOA_DATASET
class="dataset.SOADataset"
jarfile="SOADataset.jar"
inputC="dataset/OA_classification_2011.csv"
inputS="dataset/OA_to_SOA_2011.csv"
output="dataset/SOADataset"

$HOME/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class $class --master local[4] $current_folder/$jarfile $current_folder/$inputC $current_folder/$inputS $current_folder/$output