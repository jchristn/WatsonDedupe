using System;
using System.Collections.Generic;
using System.Text;

namespace WatsonDedupe
{
    /// <summary>
    /// Enumeration result.
    /// </summary>
    public class EnumerationResult
    {
        /// <summary>
        /// Prefix.
        /// </summary>
        public string Prefix = null;

        /// <summary>
        /// Index from which the enumeration should begin.
        /// </summary>
        public int IndexStart
        {
            get
            {
                return _IndexStart;
            }
            set
            {
                if (value < 0) throw new ArgumentException("Index start must be zero or greater.");
                _IndexStart = value;
            }
        }

        /// <summary>
        /// IndexStart value that should be used to continue enumeration.
        /// </summary>
        public int NextIndexStart
        {
            get
            {
                return _NextIndexStart;
            }
            set
            {
                if (value < 0) throw new ArgumentException("Next index start must be zero or greater.");
                _NextIndexStart = value;
            }
        }

        /// <summary>
        /// Maximum number of results requested.
        /// </summary>
        public int MaxResults
        {
            get
            { 
                return _MaxResults;
            }
            set
            {
                if (value < 1 || value > 100) throw new ArgumentException("Max results must be greater than zero and less than or equal to 100.");
                _MaxResults = value;
            }
        }

        /// <summary>
        /// List of object metadata.
        /// </summary>
        public List<DedupeObject> Objects = new List<DedupeObject>();

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="prefix">Prefix.</param>
        /// <param name="indexStart">Continuation token supplied in this enumeration query.</param>
        /// <param name="nextIndexStart">Continuation token that should be used to continue the enumeration.</param>
        /// <param name="maxResults">Maximum number of results requested.</param>
        /// <param name="objects">List of object metadata.</param>
        public EnumerationResult(string prefix, int indexStart, int nextIndexStart, int maxResults, List<DedupeObject> objects)
        {
            if (maxResults < 1 || maxResults > 100) throw new ArgumentException("Max results must be greater than zero and less than or equal to 100.");
            
            Prefix = prefix;
            IndexStart = indexStart;
            NextIndexStart = nextIndexStart;
            MaxResults = maxResults;
            Objects = objects;
        }

        /// <summary>
        /// Human-readable string version of the object.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string ret =
                "--- Enumeration Result ---" + Environment.NewLine +
                "    Prefix      : " + Prefix + Environment.NewLine +
                "    IndexStart  : " + IndexStart + Environment.NewLine +
                "    MaxResults  : " + MaxResults + Environment.NewLine +
                "    Objects     : " + Objects.Count;

            if (Objects.Count > 0)
            {
                foreach (DedupeObject obj in Objects)
                {
                    ret += Environment.NewLine + obj.ToString();
                }
            }

            return ret;
        }

        /// <summary>
        /// Human-readable string version of the object with tabular output for objects.
        /// </summary>
        /// <returns></returns>
        public string ToTabularString()
        {
            string ret =
                "--- Enumeration Result ---" + Environment.NewLine +
                "    Prefix      : " + Prefix + Environment.NewLine +
                "    IndexStart  : " + IndexStart + Environment.NewLine +
                "    MaxResults  : " + MaxResults + Environment.NewLine +
                "    Objects     : " + Objects.Count;

            if (Objects.Count > 0)
            { 
                ret +=
                    Environment.NewLine + Environment.NewLine +
                    "Key                                    Original     Compressed   Chunks   Maps" + Environment.NewLine +
                    "-------------------------------------- ------------ ------------ -------- --------" + Environment.NewLine;

                foreach (DedupeObject obj in Objects)
                {
                    ret +=
                        obj.Key.PadRight(38) + " " +
                        obj.OriginalLength.ToString().PadRight(12) + " " +
                        obj.CompressedLength.ToString().PadRight(12) + " " +
                        obj.Chunks.Count.ToString().PadRight(8) + " " +
                        obj.ObjectMap.Count.ToString().PadRight(8) + Environment.NewLine;
                }
            }

            return ret;
        }

        private int _IndexStart = 0;
        private int _NextIndexStart = 0;
        private int _MaxResults = 100;
    }
}
