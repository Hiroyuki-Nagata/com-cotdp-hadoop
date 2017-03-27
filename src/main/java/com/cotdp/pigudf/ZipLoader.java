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
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.pig.data.BagFactory;
import org.apache.pig.builtin.PigStreaming;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ZipLoader extends LoadFunc {

    public ZipLoader(String[] args) {
	super();
	this.delimiter = String.join("", args);
    }

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private ZipFileRecordReader reader;
    private PigStreaming ps = new PigStreaming();
    private String delimiter = "";

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
	this.reader = (ZipFileRecordReader) reader;
    }

    // Separate lump of bytes as List<byte[]>, with CRLF. Thank you !
    // --> How to split a byte array around a byte sequence in Java?
    // http://stackoverflow.com/a/29084734/2565527
    public static List<byte[]> tokens(byte[] array, byte[] delimiter) {
        List<byte[]> byteArrays = new ArrayList<byte[]>();
        if (delimiter.length == 0) {
            return byteArrays;
        }
        int begin = 0;

        outer:
        for (int i = 0; i < array.length - delimiter.length + 1; i++) {
            for (int j = 0; j < delimiter.length; j++) {
                if (array[i + j] != delimiter[j]) {
                    continue outer;
                }
            }
            byteArrays.add(Arrays.copyOfRange(array, begin, i));
            begin = i + delimiter.length;
        }
        byteArrays.add(Arrays.copyOfRange(array, begin, array.length));
        return byteArrays;
    }

    // [Pig-user] UDF Loader - one line in input result in multiple tuples
    // --> http://grokbase.com/t/pig/user/10avxg78ts/udf-loader-one-line-in-input-result-in-multiple-tuples
    // This will split files with per lines as bag
    @Override
    public Tuple getNext() throws IOException {
	try {
	    if (reader.nextKeyValue()) {
		BytesWritable bc = reader.getCurrentValue();

		if (delimiter != null && !delimiter.isEmpty()) {
		    // delimiter is set, return a tuple holds bags
		    List<byte[]> byteList = tokens(bc.getBytes(), delimiter.getBytes());
		    List<Tuple> tuples = new ArrayList<Tuple>();

		    for (int i = 0; i < byteList.size() - 1; i++) {
			tuples.add(ps.deserialize(byteList.get(i)));
		    }
		    return TupleFactory.getInstance()
			.newTuple(BagFactory.getInstance().newDefaultBag(tuples));

		} else {
		    // delimiter is empty, return a tuple
		    return ps.deserialize(bc.getBytes());
		}
	    } else {
		return null;
	    }
	} catch (InterruptedException e) {
	    throw new IOException(e);
	}
    }
}
