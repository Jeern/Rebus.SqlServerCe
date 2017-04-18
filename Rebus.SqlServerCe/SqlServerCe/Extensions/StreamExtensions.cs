using System;
using System.IO;

namespace Rebus.SqlServerCe.Extensions
{
    /// <summary>
    /// Extensions for the Stream object
    /// </summary>
    public static class StreamExtensions
    {
        /// <summary>
        /// Converts a Stream to a Byte array
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static byte[] ToByteArray(this Stream input)
        {
            using (var ms = new MemoryStream())
            {
                input.CopyTo(ms);
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Converts a Stream to a Base64 string
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static string ToBase64String(this Stream input)
        {
            return Convert.ToBase64String(input.ToByteArray());
        }
    }
}
