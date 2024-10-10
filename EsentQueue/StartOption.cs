namespace EsentQueue;

/// <summary>
/// Options to define on start of the queue
/// </summary>
public enum StartOption
{
   /// <summary>
   /// Create a new empty database.
   /// </summary>
   CreateNew,
   /// <summary>
   /// Open an existing database or create a new one if not.
   /// </summary>
   OpenOrCreate
}
