/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ohdsi.webapi.txpath;

import java.util.Comparator;
import java.util.List;


// Reference: https://stackoverflow.com/questions/4859261/get-the-indices-of-an-array-after-sorting
public class ArrayIndexComparator implements Comparator<Integer>
{
	private final List<Integer> array;

	public ArrayIndexComparator(List<Integer> array)
	{
			this.array = array;
	}

	public Integer[] createIndexArray()
	{
			Integer[] indexes = new Integer[array.size()];
			for (int i = 0; i < array.size(); i++)
			{
					indexes[i] = i; // Autoboxing
			}
			return indexes;
	}

	@Override
	public int compare(Integer index1, Integer index2)
	{
		return array.get(index1).compareTo(array.get(index2));
	}
}
