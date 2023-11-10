using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sorting
{
  // Klassen sorterar en array genom att använda flera trådar samtidigt.
  public class ParallelSort<T> : ISort<T>
  {
    // Sorterings algoritmens namn och identifierare
    public string Name => "ParallelSort";

    // Standard sorteringsmetoden, som använder standard comparer
    public void Sort(T[] inputOutput)
    {
      Sort(inputOutput, Comparer<T>.Default);
    }

    // Sorteringsmetod som använder comparer för att jämföra object dvs chunks
    public void Sort(T[] inputOutput, IComparer<T> comparer)
    {

      // int som håller antalet kärnor miljön har, dvs hur många kärnor datorns processor har
      int cores = Environment.ProcessorCount;
      // Ränkar ut storleken av varje chunk, genom att dividera längden på arrayen med antalet kärnor
      // inputOutput.Length + cores - 1, istället för inputOutput.Length / cores, så att varje element i arrayen blir inkluderade i en chunk, det skapar lite extra utrymme i varje chunk så att "remaindern" också kan bli includerad i chunksen
      int chunkSize = (inputOutput.Length + cores - 1) / cores; 

      // Array som används för att hålla tasks, en task för varje kärna, och arrayens längd bestäms av antalet kärnor
      Task[] tasks = new Task[cores];
      for (int i = 0; i < cores; i++)
      {
        // Start index för chunken, genom att mulitplicera i med chunksize så får vi start index av alla chunksen efter varandra, på detta sätt delegerar vi till varje kärna vart de ska börja arbeta
        int start = i * chunkSize;
        // Slut index för chunken, inputOutput.Length används här för att se till att slut index inte överstiger det totala antalet element i arrayen
        int end = Math.Min(start + chunkSize, inputOutput.Length);
        // Task för att sortera varje chunk av arrayen parallelt, detta genom att den först tar in arrayen som ska sorteras "inputOutput" och sedan sorterar från start (start index) och sedan sorterar det antalet element som angivits ska sorteras, det får den genom "end - start", sedan används comparer för att sköta sorteringen
        // Genom att använda Task.Run delegeras varje sorterings operation till en separat tråd, vilket tillåter att flera chunks blir sorterade samtidigt
        tasks[i] = Task.Run(() => Array.Sort(inputOutput, start, end - start, comparer));
      }
      
      // Vänta tills alla tasks är slutförda
      Task.WaitAll(tasks);
      
      // Merga alla sorterade chunks till den ursprungliga arrayen
      MergeSortedChunks(inputOutput, cores, chunkSize, comparer);
    }

    // Metod för att merga de sorterade chunkesn från varje task till en sorterad array
    private void MergeSortedChunks(T[] inputOutput, int cores, int chunkSize, IComparer<T> comparer)
    {

      // Array för att hålla de mergade och sorterade elementen
      T[] merged = new T[inputOutput.Length];

      // Använder mig av SortedSet som själv sorterar sitt innehåll, med det minsta först och största sist
      // value representerar värdet vi ska jämföra, chunkindex representerar vilken chunk som value är i, och elementIndex representerar vilket index i chunken som value sitter på
      // comparer jämför det minsta värdet i varje chunkarray för att bestämma i vilken ordning de ska vara i den slutgiltiga mergade arrayen
      // Vi fortsätter att plocka ut de minsta värdena från chunkarrayerna tills att dessa är tomma
      var minChunk = new SortedSet<(T value, int chunkIndex, int elementIndex)>(Comparer<(T value, int chunkIndex, int elementIndex)>.Create((a, b) =>
      {

        int compare = comparer.Compare(a.value, b.value);
        if (compare != 0) return compare;
        return a.chunkIndex.CompareTo(b.chunkIndex);
      })
      );

      // Skapar sorted set med det första elementet av varje chunk, varje chunk är redan sorterad, so första värdet av varje chunk är oche dess minsta värde
      for(int i = 0; i < cores; i++)
      {
        int start = i * chunkSize;
        if(start < inputOutput.Length)
        {
          minChunk.Add((inputOutput[start], i, start + 1));
        }
      }

      // Vi itererar genom hela längden av den ursprungliga arrayen för att bygga den sorterade merged arrayen
      for(int i = 0; i < inputOutput.Length; i++)
      {
        // Får minsta värdet av varje sortedset
        var (value, chunkIndex, elementIndex) = minChunk.Min;

        // När vi har fått värdet så tar vi bort det från sortedsettet för att upreppa processen
        minChunk.Remove(minChunk.Min);
        // Vi lägger till det minsta värdet till den mergade arrayen 
        merged[i] = value;

        // Räknar ut start och slut index för nästa element från chunken
        int nextChunkStart = chunkIndex * chunkSize;
        int nextChunkEnd = Math.Min(nextChunkStart + chunkSize, inputOutput.Length);
        
        // Om inte chunken är tom så läggs nästa dvs det minsta värdet av chunken (eftersom alla chunks är sorterade med minst först) till sortedsettet
        if(elementIndex < nextChunkEnd)
        {
          minChunk.Add((inputOutput[elementIndex], chunkIndex, elementIndex + 1));
        }
      }
      // Använder oss av Copy för att kopiera från source arrayen "merged" till destination arrayen "inputOutput" där vi också deklarerar hur många element som ska kopieras över med hjälp av "merged.Length"
      Array.Copy(merged, inputOutput, merged.Length);
    }
  }
}
