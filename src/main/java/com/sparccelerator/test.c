#include <jni.h>
#include <stdio.h>

JNIEXPORT jint JNICALL Java_com_sparccelerator_CustomMapPart_testC
(JNIEnv* env, jobject input, jobject buf1, jint buf1Off, jint buf1Len)
{

struct __attribute__((__packed__)) tuple5 {
    int first_field;
    int second_field;
    int third_field;
    int fourth_field;
    int fifth_field;

};
struct tuple5 *tuples;

int i,n,j;
jbyte* _buf1;


tuples = (*env)->GetDirectBufferAddress(env, buf1);
n = buf1Len;


int sum = 0;
for (i = 0; i < n; i++)
{
    sum+= (tuples+i)->first_field;
    sum+= (tuples+i)->second_field;

}

return sum;
}