/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.sysml.runtime.matrix.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.sysml.runtime.instructions.mr.GroupedAggregateInstruction;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.MatrixValue;
import org.apache.sysml.runtime.matrix.data.TaggedInt;
import org.apache.sysml.runtime.matrix.data.WeightedCell;


public class GroupedAggMRMapper extends MapperBase
	implements Mapper<MatrixIndexes, MatrixValue, TaggedInt, WeightedCell>
{
		
	//block instructions that need to be performed in part by mapper
	protected ArrayList<ArrayList<GroupedAggregateInstruction>> groupAgg_instructions=new ArrayList<ArrayList<GroupedAggregateInstruction>>();
	private IntWritable outKeyValue=new IntWritable();
	private TaggedInt outKey=new TaggedInt(outKeyValue, (byte)0);
	private WeightedCell outValue=new WeightedCell();

	@Override
	public void map(MatrixIndexes key, MatrixValue value,
			        OutputCollector<TaggedInt, WeightedCell> out, Reporter reporter) 
	    throws IOException 
	{
		for(int i=0; i<representativeMatrixes.size(); i++)
			for(GroupedAggregateInstruction ins : groupAgg_instructions.get(i))
			{
				//set the tag once for the block
				outKey.setTag(ins.output);
				
				//get block and unroll into weighted cells
				//(it will be in dense format)
				MatrixBlock block = (MatrixBlock) value;
				
				int rlen = block.getNumRows();
				int clen = block.getNumColumns();
				if( clen == 2 ) //w/o weights
				{
					for( int r=0; r<rlen; r++ )
					{
						outKeyValue.set((int)block.quickGetValue(r, 1));
						outValue.setValue(block.quickGetValue(r, 0));
						outValue.setWeight(1);
						out.collect(outKey, outValue);		
					}
				}
				else //w/ weights
				{
					for( int r=0; r<rlen; r++ )
					{
						outKeyValue.set((int)block.quickGetValue(r, 1));
						outValue.setValue(block.quickGetValue(r, 0));
						outValue.setWeight(block.quickGetValue(r, 2));
						out.collect(outKey, outValue);		
					}
				}
			}
	}

	@Override
	protected void specialOperationsForActualMap(int index,
			OutputCollector<Writable, Writable> out, Reporter reporter)
			throws IOException 
	{
		
	}
	
	@Override
	public void configure(JobConf job)
	{
		super.configure(job);
		
		try 
		{
			GroupedAggregateInstruction[] grpaggIns = MRJobConfiguration.getGroupedAggregateInstructions(job);
			if( grpaggIns == null )
				throw new RuntimeException("no GroupAggregate Instructions found!");
			
			ArrayList<GroupedAggregateInstruction> vec = new ArrayList<GroupedAggregateInstruction>();
			for(int i=0; i<representativeMatrixes.size(); i++)
			{
				byte input=representativeMatrixes.get(i);
				for(GroupedAggregateInstruction ins : grpaggIns)
					if(ins.input == input)
						vec.add(ins);
				groupAgg_instructions.add(vec);
			}
		} 
		catch (Exception e) 
		{
			throw new RuntimeException(e);
		} 
	}
}
