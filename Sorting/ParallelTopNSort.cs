using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace Sorting
{
  public class ParallelTopNSort<T> : ITopNSort<T>
  {
    public string Name => "ParallelTopNSort";

    public T[] TopNSort(T[] inputOutput, int n)
    {
      return TopNSort(inputOutput, n, Comparer<T>.Default);
    }

    public T[] TopNSort(T[] inputOutput, int n, IComparer<T> comparer)
    {
      if ( n <=0 || inputOutput.Length == 0)
      {
        return new T[0];
      }

      n = Math.Min(n, inputOutput.Length);

      int cores = Environment.ProcessorCount;
      int chunkSize = (inputOutput.Length + cores - 1) / cores;

      T[][] sortedChunks = new T[cores][];
      Task[] tasks = new Task[cores];

      for (int i = 0; i < cores; i++)
      {
        int localIndex = i; 
        tasks[i] = Task.Run(() =>
        {
          int start = localIndex * chunkSize;
          int end = (localIndex == cores - 1) ? inputOutput.Length : start + chunkSize;
          int chunkLength = end - start;

          T[] chunk = new T[chunkLength];
          Array.Copy(inputOutput, start, chunk, 0, chunkLength);
          Array.Sort(chunk, comparer);
          sortedChunks[localIndex] = chunk;
        });
      }

      Task.WaitAll(tasks);

      T[] topN = MergeSortedChunks(sortedChunks, n, comparer);
      return topN;
    }

    public class MinElements<TValue, TComparer> where TComparer : IComparer<TValue>
    {
      private List<TValue> elements = new List<TValue>();
      private List<int> chunkIndices = new List<int>();
      private List<int> elementIndices = new List<int>();
      private TComparer comparer;

      public MinElements(TComparer comparer)
      {
        this.comparer = comparer;
      }

      public int Count => elements.Count;

      public (TValue value, int chunkIndex, int elementIndex) RemoveMin()
      {
        if (elements.Count == 0)
        {
          throw new InvalidOperationException("Heap is empty");
        }
        var minValue = elements[0];
        var minChunkIndex = chunkIndices[0];
        var minElementIndex = elementIndices[0];
        Swap(0, elements.Count - 1);
        elements.RemoveAt(elements.Count - 1);
        chunkIndices.RemoveAt(chunkIndices.Count - 1);
        elementIndices.RemoveAt(elementIndices.Count - 1);
        elementsDown(0);
        return (minValue, minChunkIndex, minElementIndex);
      }

      public void Add(TValue value, int chunkIndex, int elementIndex)
      {
        elements.Add(value);
        chunkIndices.Add(chunkIndex);
        elementIndices.Add(elementIndex);
        elementsUp(elements.Count - 1);
      }

      private void elementsUp(int index)
      {
        while (index > 0)
        {
          int parentIndex = (index - 1) / 2;
          if (comparer.Compare(elements[index], elements[parentIndex]) >= 0)
          {
            break;
          }
          Swap(index, parentIndex);
          index = parentIndex;
        }
      }

      private void elementsDown(int index)
      {
        int smallest = index;
        int leftChildIndex = 2 * index + 1;
        int rightChildIndex = 2 * index + 2;

        if (leftChildIndex < elements.Count && comparer.Compare(elements[leftChildIndex], elements[smallest]) < 0)
        {
          smallest = leftChildIndex;
        }

        if (rightChildIndex < elements.Count && comparer.Compare(elements[rightChildIndex], elements[smallest]) < 0)
        {
          smallest = rightChildIndex;
        }

        if (smallest != index)
        {
          Swap(index, smallest);
          elementsDown(smallest);
        }
      }

      private void Swap(int index1, int index2)
      {
        (elements[index1], elements[index2]) = (elements[index2], elements[index1]);
        (chunkIndices[index1], chunkIndices[index2]) = (chunkIndices[index2], chunkIndices[index1]);
        (elementIndices[index1], elementIndices[index2]) = (elementIndices[index2], elementIndices[index1]);
      }

    }

    private T[] MergeSortedChunks(T[][] sortedChunks, int n, IComparer<T> comparer)
    {
      T[] result = new T[n];
      var minElements = new MinElements<T, IComparer<T>>(comparer);

      for (int i = 0; i < sortedChunks.Length; i++)
      {
        if (sortedChunks[i].Length > 0)
        {
          minElements.Add(sortedChunks[i][0], i, 0);
        }
      }

      for (int i = 0; i < n; i++)
      {
        if (minElements.Count == 0)
        {
          break;
        }

        var (value, chunkIndex, elementIndex) = minElements.RemoveMin();
        result[i] = value;

        if (elementIndex + 1 < sortedChunks[chunkIndex].Length)
        {
          minElements.Add(sortedChunks[chunkIndex][elementIndex + 1], chunkIndex, elementIndex + 1);
        }
      }

      return result;
    }
  }
}
