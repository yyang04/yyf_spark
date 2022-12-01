package utils
import com.google.common.hash.Hashing
import java.nio.charset.StandardCharsets.UTF_8

object Murmurhash {
    def hashString[T](prefix: String, input: T) : Long = {
        val feature = prefix + input
        var hash = Hashing.murmur3_32().hashString(feature, UTF_8).asInt().toLong
        if (hash < 0) {
            hash = hash + 1L << 32
        }
        hash % (1L << 31)
    }
}
