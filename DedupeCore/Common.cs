using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace WatsonDedupe
{
    /// <summary>
    /// Commonly-used static methods.
    /// </summary>
    public static class Common
    {
        public static string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return String.Empty;

            string ret = "";
            int doubleDash = 0;
            int openComment = 0;
            int closeComment = 0;

            for (int i = 0; i < s.Length; i++)
            {
                if (((int)(s[i]) == 10) ||      // Preserve carriage return
                    ((int)(s[i]) == 13))        // and line feed
                {
                    ret += s[i];
                }
                else if ((int)(s[i]) < 32)
                {
                    continue;
                }
                else
                {
                    ret += s[i];
                }
            }

            //
            // double dash
            //
            doubleDash = 0;
            while (true)
            {
                doubleDash = ret.IndexOf("--");
                if (doubleDash < 0)
                {
                    break;
                }
                else
                {
                    ret = ret.Remove(doubleDash, 2);
                }
            }

            //
            // open comment
            // 
            openComment = 0;
            while (true)
            {
                openComment = ret.IndexOf("/*");
                if (openComment < 0) break;
                else
                {
                    ret = ret.Remove(openComment, 2);
                }
            }

            //
            // close comment
            //
            closeComment = 0;
            while (true)
            {
                closeComment = ret.IndexOf("*/");
                if (closeComment < 0) break;
                else
                {
                    ret = ret.Remove(closeComment, 2);
                }
            }

            //
            // in-string replacement
            //
            ret = ret.Replace("'", "''");
                    
            return ret;
        }

        public static byte[] Sha1(byte[] data)
        {
            SHA1Managed s = new SHA1Managed();
            return s.ComputeHash(data);
        }

        public static byte[] Sha256(byte[] data)
        {
            SHA256Managed s = new SHA256Managed();
            return s.ComputeHash(data);
        }

        public static byte[] Md5(byte[] data)
        {
            MD5 m = MD5.Create();
            return m.ComputeHash(data);
        }
        
        public static byte[] Base64ToBytes(string data)
        {
            if (String.IsNullOrEmpty(data)) return null;
            data = data.Replace("_", "/").Replace("-", "+");

            while (data.Length % 4 != 0)
            {
                data = data + "=";
            }

            return Convert.FromBase64String(data);
        }

        public static string Base64ToString(string data)
        {
            if (String.IsNullOrEmpty(data)) return null;
            byte[] bytes = System.Convert.FromBase64String(data);
            return System.Text.UTF8Encoding.UTF8.GetString(bytes);
        }

        public static string BytesToBase64(byte[] data)
        {
            if (data == null) return null;
            if (data.Length < 1) return null;
            string temp = System.Convert.ToBase64String(data);
            temp = temp.Replace("=", "").Replace("/", "_").Replace("+", "-");
            return temp;
        }

        public static bool IsZeroBytes(byte[] data, int numBytes)
        {
            if (data == null || data.Length < 1) return true;
            byte[] testData = new byte[numBytes];

            for (int i = 0; i < numBytes; i++)
            {
                if (data.Length >= i)
                {
                    testData[i] = data[i];
                }
                else
                {
                    testData[i] = 0x00;
                }
            }

            for (int i = 0; i < numBytes; i++)
            {
                if (testData[i] != 0x00) return false;
            }

            return true;
        }

        public static void ClearCurrentLine()
        {
            int currLineCursor = Console.CursorTop;
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write(new string(' ', Console.WindowWidth));
            Console.SetCursorPosition(0, currLineCursor);
        }

        public static byte[] ShiftLeft(byte[] data, int shiftCount, byte fill)
        {
            if (data == null || data.Length < 1) return null;
            byte[] ret = InitBytes(data.Length, fill);
            if (data.Length < shiftCount) return ret;

            Buffer.BlockCopy(data, shiftCount, ret, 0, (data.Length - shiftCount));
            return ret;
        }

        public static byte[] ShiftRight(byte[] data, int shiftCount, byte fill)
        {
            if (data == null || data.Length < 1) return null;
            byte[] ret = InitBytes(data.Length, fill);
            if (data.Length < shiftCount) return ret;

            Buffer.BlockCopy(data, 0, ret, shiftCount, (data.Length - shiftCount));
            return ret;
        }

        public static byte[] InitBytes(int count, byte val)
        {
            byte[] ret = new byte[count];
            for (int i = 0; i < ret.Length; i++)
            {
                ret[i] = val;
            }
            return ret;
        }
        
        public static bool InputBoolean(string question, bool yesDefault)
        {
            Console.Write(question);

            if (yesDefault) Console.Write(" [Y/n]? ");
            else Console.Write(" [y/N]? ");

            string userInput = Console.ReadLine();

            if (String.IsNullOrEmpty(userInput))
            {
                if (yesDefault) return true;
                return false;
            }

            userInput = userInput.ToLower();

            if (yesDefault)
            {
                if (
                    (String.Compare(userInput, "n") == 0)
                    || (String.Compare(userInput, "no") == 0)
                   )
                {
                    return false;
                }

                return true;
            }
            else
            {
                if (
                    (String.Compare(userInput, "y") == 0)
                    || (String.Compare(userInput, "yes") == 0)
                   )
                {
                    return true;
                }

                return false;
            }
        }

        public static List<string> InputStringList(string question)
        {
            Console.WriteLine("Press ENTER with no data to end");
            List<string> ret = new List<string>();
            while (true)
            {
                Console.Write(question + " ");
                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput)) break;
                ret.Add(userInput);
            }
            return ret;
        }

        public static string InputString(string question, string defaultAnswer, bool allowNull)
        {
            while (true)
            {
                Console.Write(question);

                if (!String.IsNullOrEmpty(defaultAnswer))
                {
                    Console.Write(" [" + defaultAnswer + "]");
                }

                Console.Write(" ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    if (!String.IsNullOrEmpty(defaultAnswer)) return defaultAnswer;
                    if (allowNull) return null;
                    else continue;
                }

                return userInput;
            }
        }

        public static int InputInteger(string question, int defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                int ret = 0;
                if (!Int32.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid integer.");
                    continue;
                }

                if (ret == 0)
                {
                    if (allowZero)
                    {
                        return 0;
                    }
                }

                if (ret < 0)
                {
                    if (positiveOnly)
                    {
                        Console.WriteLine("Please enter a value greater than zero.");
                        continue;
                    }
                }

                return ret;
            }
        }

        public static decimal InputDecimal(string question, decimal defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                decimal ret = 0;
                if (!Decimal.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid decimal.");
                    continue;
                }

                if (ret == 0)
                {
                    if (allowZero)
                    {
                        return 0;
                    }
                }

                if (ret < 0)
                {
                    if (positiveOnly)
                    {
                        Console.WriteLine("Please enter a value greater than zero.");
                        continue;
                    }
                }

                return ret;
            }
        }

        public static string DecimalToString(object obj)
        {
            if (obj == null) return null;
            string ret = string.Format("{0:N2}", obj);
            ret = ret.Replace(",", "");
            return ret;
        }
    }
}