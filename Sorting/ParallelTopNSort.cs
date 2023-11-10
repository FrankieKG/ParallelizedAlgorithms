using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace Sorting
{
  // Klassen parallell sorterar en Array och hittar de N bästa/sämsta element
  public class ParallelTopNSort<T> : ITopNSort<T>
  {
    public string Name => "ParallelTopNSort";

    public T[] TopNSort(T[] inputOutput, int n)
    {
      return TopNSort(inputOutput, n, Comparer<T>.Default);
    }

    // Metod för att sortera och hämta de N bästa/sämsta element med en anpassad jämförare
    public T[] TopNSort(T[] inputOutput, int n, IComparer<T> comparer)
    {
      // Kolla för att se till att indatan är giltig
      if ( n <=0 || inputOutput.Length == 0)
      {
        return new T[0];
      }

      // Begränsar "n" till antalet element som finns i arrayen
      n = Math.Min(n, inputOutput.Length);

      // Hämtar antalet kärnor miljön har
      int cores = Environment.ProcessorCount;

      // Beräknar storleken på varje chunk, beroende på hur många kärnor det finns att jobba med
      int chunkSize = (inputOutput.Length + cores - 1) / cores;

      // Array för att hålla uppgifter för varje kärna
      T[][] sortedChunks = new T[cores][];

      // Array för att parallella operationer dvs tasks
      Task[] tasks = new Task[cores];

      // Skapar och startar en task för varje chunk inför parallell sortering
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

      // Väntar tills alla tasks är klara
      Task.WaitAll(tasks);

      // Sammanfogar de N bästa elementen tvärs över alla sorterade chunks
      T[] topN = MergeSortedChunks(sortedChunks, n, comparer);
      return topN;
    }

    // Klassen hanterar en min-heap för att hitta och plocka ut de minsta elementen från de sorterade chunksen, eftersom versionen 4.7.2 a .NET används, så kan vi inte utnyttja PriorityQueue från .NET 5, och är tvungna att improvisera
    public class MinElements<TValue, TComparer> where TComparer : IComparer<TValue>
    {
      // Lagrar element i heapen
      private List<TValue> elements = new List<TValue>();
      // Spårar vilken chunk varje element kommer ifrån
      private List<int> chunkIndices = new List<int>();
      // Spårar index för varje element inom dess chunk
      private List<int> elementIndices = new List<int>();
      // Jämförare som används för att jämföra element i heapen
      private TComparer comparer;

      //konstruktor för att initiera jämföraren
      public MinElements(TComparer comparer)
      {
        this.comparer = comparer;
      }

      // Returnerar antalet element som finns i heapen
      public int Count => elements.Count;

      // Tar bort och returnerar det minsta elementet i heapen
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

      // Lägger till element i heapen
      public void Add(TValue value, int chunkIndex, int elementIndex)
      {
        elements.Add(value);
        chunkIndices.Add(chunkIndex);
        elementIndices.Add(elementIndex);
        elementsUp(elements.Count - 1);
      }

      // Metod för att flytta upp ett element i heapen
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

      // Metod för att flytta ner ett element i heapen
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

      // Metod för att byta plats på två element i heapen
      private void Swap(int index1, int index2)
      {
        (elements[index1], elements[index2]) = (elements[index2], elements[index1]);
        (chunkIndices[index1], chunkIndices[index2]) = (chunkIndices[index2], chunkIndices[index1]);
        (elementIndices[index1], elementIndices[index2]) = (elementIndices[index2], elementIndices[index1]);
      }

    }

    // Metod för att sammanfoga sorterade chunks och hitta de N bästa/sämsta elementen
    private T[] MergeSortedChunks(T[][] sortedChunks, int n, IComparer<T> comparer)
    {
      // Array för att lagra de N elementen
      T[] result = new T[n];
      
      // Skapar en instans av MinElements för att hantera och plocka ut de minsta elementen
      var minElements = new MinElements<T, IComparer<T>>(comparer);

      // Lägger till de första elementen från varje sorterad chunk i minElements
      for (int i = 0; i < sortedChunks.Length; i++)
      {
        if (sortedChunks[i].Length > 0)
        {
          minElements.Add(sortedChunks[i][0], i, 0);
        }
      }

      // Samlar ihop de N bästa elementen
      for (int i = 0; i < n; i++)
      {
        // Kollar om minElements är tom
        if (minElements.Count == 0)
        {
          break;
        }

        // Tar bort det minsta elementet samt dess chunk- och elementindex från minElements
        var (value, chunkIndex, elementIndex) = minElements.RemoveMin();
        // Lägger till minsta elementet i result-arrayen
        result[i] = value;

        // Kollar om chunken är tom
        if (elementIndex + 1 < sortedChunks[chunkIndex].Length)
        {
          // Lägger till nästa element från samma chunk i minElements
          minElements.Add(sortedChunks[chunkIndex][elementIndex + 1], chunkIndex, elementIndex + 1);
        }
      }
      // Returnerar arrayen med de N bästa elementen
      return result;
    }
  }
}
