#!/bin/bash
#PBS -A train_bigdat16
#PBS -l walltime=05:00:00
#PBS -l select=1:ncpus=20:mem=64GB
#PBS -q parallel
#PBS -p 1020
module load autoload jre/1.8.0_73
current_folder=`pwd`/bd

#MACHINE-LEARNING_DATASET
class="dataset.MachineLearningDataset"
jarfile="MachineLearningDataset.jar"
inputSOA="dataset/SOADataset"
inputSTREET="dataset/street"
output="dataset/MLDataset"

$HOME/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class $class --master local[4] $current_folder/$jarfile $current_folder/$inputSOA $current_folder/$inputSTREET $current_folder/$output