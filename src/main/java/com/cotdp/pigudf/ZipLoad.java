/**
 * Copyright 2017 Hiroyuki Nagata <idiotpanzer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cotdp.pigudf;

import com.cotdp.hadoop.ZipFileInputFormat;
import com.cotdp.hadoop.ZipFileRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ZipLoad extends LoadFunc {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private ZipFileRecordReader reader;
    private List<Object> cachedList;

    @SuppressWarnings("rawtypes")
    @Override
    public ZipFileInputFormat getInputFormat() throws IOException {
	return new ZipFileInputFormat();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
	Path path = new Path(location);
	ZipFileInputFormat.setInputPaths(job, path);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
	throws IOException {
	this.reader     = (ZipFileRecordReader) reader;
	this.cachedList = new ArrayList<Object>();
    }

    @Override
    public Tuple getNext() throws IOException {
	return null;
    }
}
