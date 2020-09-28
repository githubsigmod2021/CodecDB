#include <stdint.h>
#include <stdlib.h>
#include "edu_uchicago_cs_encsel_query_offheap_EqualJniScalar.h"

void scanByByte(jbyte* input, jint offset, jint idx, jint target, jint entryWidth, jbyte* result, jlong& buffer);
void scanInt(jbyte* input, jlong len, jint offset, jint size, jint target, jint entryWidth, jbyte* result);
void scanLong(jbyte* input, jlong len, jint offset, jint size, jint target, jint entryWidth, jbyte* result);

jint getInt(jbyte* buffer, jint index) ;
jlong getLong(jbyte* buffer, jint index) ;
void putInt(jbyte* buffer, jint index, jint value);
void putLong(jbyte* buffer, jint index, jlong value) ;


static void init() __attribute__((constructor));
static void release() __attribute__((destructor));

static jbyte* memblock;
static uint64_t memsize;

void init() {
    memsize = 100*1024*1024;
    memblock = (jbyte*)malloc(memsize*sizeof(jbyte));
}

void release() {
    free(memblock);
}

JNIEXPORT jobject JNICALL Java_edu_uchicago_cs_encsel_query_offheap_EqualJniScalar_executeDirect
  (JNIEnv *env, jobject self, jobject input, jint offset, jint size, jint target, jint entryWidth) {
   jlong resSize = (size * entryWidth / 64 + (((size * entryWidth) % 64)?1:0)) * 8;

   if(resSize > memsize) {
        memsize = resSize;
        free(memblock);
        memblock = (jbyte*) malloc(sizeof(jbyte)*memsize);
   }

   jbyte* array = (jbyte*)env->GetDirectBufferAddress(input);
   jlong arrayLen = env->GetDirectBufferCapacity(input);

   if(entryWidth < 26)
       scanInt(array, arrayLen, offset, size, target, entryWidth, memblock);
   else
       scanLong(array,arrayLen, offset, size, target, entryWidth, memblock);

   return env->NewDirectByteBuffer(memblock,resSize);
}

/*
 * Class:     edu_uchicago_cs_encsel_query_offheap_EqualSJniScalar
 * Method:    executeHeap
 * Signature: ([BIIII)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_edu_uchicago_cs_encsel_query_offheap_EqualJniScalar_executeHeap
  (JNIEnv *env, jobject self, jbyteArray input, jint offset, jint size, jint target, jint entryWidth) {

    jlong resSize = (size * entryWidth / 64 + (((size * entryWidth) % 64)?1:0)) * 8;

    if(resSize > memsize) {
           memsize = resSize;
           free(memblock);
           memblock = (jbyte*) malloc(sizeof(jbyte)*memsize);
      }
    jbyte* array = env->GetByteArrayElements(input, 0);
    jint arrayLen = env->GetArrayLength(input);

    if(entryWidth < 26)
        scanInt(array, arrayLen, offset, size, target, entryWidth, memblock);
    else
        scanLong(array,arrayLen, offset, size, target, entryWidth, memblock);
    return env->NewDirectByteBuffer(memblock,resSize);
}

void scanInt(jbyte* input, jlong len, jint offset, jint size, jint target, jint entryWidth, jbyte* result) {
    jlong buffer = 0;
    // Entry before this number can be read using getInt
    jint entryInInt = (len - offset - 4) * 8 / entryWidth + 1;
    jint intLimit = entryInInt < size? entryInInt : size;
    jint mask = (1 << entryWidth) - 1;

    for (int i = 0; i < intLimit; i++) {
      jint byteIndex = i * entryWidth / 8;
      jint byteOffset = i * entryWidth % 8;
      jint readValue = getInt(input, offset + byteIndex) >> byteOffset;

      if ((readValue & mask) == target) {
          buffer |= (1L << (i % 64));
      }

      if (i % 64 == 63) {
        putLong(result, i/64*8, buffer);
        buffer = 0;
      }
    }

    // These entries need to be read bytes by bytes
    for (int i = intLimit ; i < size; i++) {
      scanByByte(input, offset, i, target, entryWidth, result, buffer);
    }
    if (size % 64 != 0)
       putLong(result, size/64*8, buffer);

}

void scanLong(jbyte* input, jlong len, jint offset, jint size, jint target, jint entryWidth, jbyte* result) {
    jlong buffer = 0;
    // Entry before this number can be read using getInt
    jint entryInLong = (len - offset - 8) * 8 / entryWidth + 1;
    jint longLimit = entryInLong < size? entryInLong : size;
    jlong mask = (1L << entryWidth) - 1;

    for (int i = 0; i < longLimit; i++) {
      jint byteIndex = i * entryWidth / 8;
      jint byteOffset = i * entryWidth % 8;

      jlong readValue = getLong(input, offset + byteIndex) >> byteOffset;

      if ((readValue & mask) == target) {
          buffer |= (1L << (i % 64));
      }

      if (i % 64 == 63) {
        putLong(result, i/64*8, buffer);
        buffer = 0;
      }
    }
    // These entries need to be read bytes by bytes
    for (int i = longLimit ; i < size; i++) {
       scanByByte(input, offset, i, target, entryWidth, result, buffer);
    }
    if (size % 64 != 0)
       putLong(result, size/64*8, buffer);
}

void scanByByte(jbyte* input, jint offset, jint idx, jint target, jint entryWidth, jbyte* result, jlong& buffer) {
    jint byteIndex = idx * entryWidth / 8;
    jint byteOffset = idx * entryWidth % 8;

    jint byteToRead = (byteOffset + entryWidth) / 8 + ((byteOffset + entryWidth ) % 8?1:0);

    jlong readValue = 0L;

    for (int i = 0;i< byteToRead;i++) {
      jint readByte = *(input + offset + byteIndex + i) & 0xff;
      readValue |= readByte << (i * 8);
    }
    readValue >>= byteOffset;
    jlong mask = (1L << entryWidth) - 1;

    if((readValue & mask) == target) {
        buffer |= (1L << idx % 64);
    }

    if (idx % 64 == 63) {
      putLong(result, idx/64*8, buffer);
      buffer = 0;
    }
  }

jint getInt(jbyte* buffer, jint index) {
    jint result = *(buffer+index) & 0xff;
    result |= (*(buffer+index+1) & 0xff) << 8;
    result |= (*(buffer+index+2) & 0xff) << 16;
    result |= (*(buffer+index+3) & 0xff) << 24;
    return result;
}

jlong getLong(jbyte* buffer, jint index) {
    jlong result = (jlong)*(buffer+index) & 0xff;
    result |= ((jlong)*(buffer+index+1) &0xff) << 8;
    result |= ((jlong)*(buffer+index+2) & 0xff) << 16;
    result |= ((jlong)*(buffer+index+3) & 0xff) << 24;
    result |= ((jlong)*(buffer+index+4) & 0xff) << 32;
    result |= ((jlong)*(buffer+index+5) & 0xff) << 40;
    result |= ((jlong)*(buffer+index+6) & 0xff) << 48;
    result |= ((jlong)*(buffer+index+7) & 0xff) << 56;
    return result;
}

void putInt(jbyte* buffer, jint index, jint value) {
    *(buffer + index) = (jbyte)value;
    *(buffer + index + 1) = (jbyte)(value >> 8);
    *(buffer + index + 2) = (jbyte)(value >> 16);
    *(buffer + index + 3) = (jbyte)(value >> 24);
}

void putLong(jbyte* buffer, jint index, jlong value) {
    *(buffer + index) = (jbyte)value;
    *(buffer + index + 1) = (jbyte)(value >> 8);
    *(buffer + index + 2) = (jbyte)(value >> 16);
    *(buffer + index + 3) = (jbyte)(value >> 24);
    *(buffer + index + 4) = (jbyte)(value >> 32);
    *(buffer + index + 5) = (jbyte)(value >> 40);
    *(buffer + index + 6) = (jbyte)(value >> 48);
    *(buffer + index + 7) = (jbyte)(value >> 56);
}