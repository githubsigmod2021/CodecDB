//
// Created by harper on 3/28/20.
//

#include <gtest/gtest.h>
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <vector>
#include <memory>
#include <string>
#include <sstream>
#include "encoding.h"

namespace parquet {

    namespace test {

        using namespace std;

        TEST(DeltaBpDecodingTest, DecodeFile) {
            std::ifstream infile("testres/encoding/deltabp.txt");
            int lineval;
            vector<int32_t> buffer;
            while (infile >> lineval) {
                buffer.push_back(lineval);
            }

            struct stat st;
            stat("testres/encoding/deltabp", &st);
            auto file_size = st.st_size;
            auto fd = open("testres/encoding/deltabp", O_RDONLY, 0);
            //Execute mmap
            void *mmappedData = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            assert(mmappedData != MAP_FAILED);
            auto content = (uint8_t *) mmappedData;

            auto decoder_raw = MakeDecoder(Type::INT32, Encoding::DELTA_BINARY_PACKED, nullptr);

            auto decoder = dynamic_cast<TypedDecoder<Int32Type> *>(decoder_raw.get());
            decoder->SetData(buffer.size(), content, file_size);

            vector<int32_t> buffer2(buffer.size());
            decoder->Decode(buffer2.data(), buffer.size());

            for (uint32_t i = 0; i < buffer.size(); ++i) {
                EXPECT_EQ(buffer[i], buffer2[i]);
            }

            munmap(content, file_size);
            close(fd);
        }

        TEST(DeltaBinaryTest, Decode) {
            std::ifstream infile("testres/encoding/comment");
            string lineval;
            vector<string> strbuffer;
            vector<ByteArray> buffer;
            while (std::getline(infile, lineval)) {
                strbuffer.push_back(lineval);
                buffer.push_back(ByteArray(strbuffer.back()));
            }

            struct stat st;
            stat("testres/encoding/comment.binary", &st);
            auto file_size = st.st_size;
            auto fd = open("testres/encoding/comment.binary", O_RDONLY, 0);
            //Execute mmap
            void *mmappedData = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            assert(mmappedData != MAP_FAILED);
            auto content = (uint8_t *) mmappedData;

            auto decoder_raw = MakeDecoder(Type::BYTE_ARRAY, Encoding::DELTA_BYTE_ARRAY, nullptr);

            auto decoder = dynamic_cast<TypedDecoder<ByteArrayType> *>(decoder_raw.get());
            decoder->SetData(buffer.size(), content, file_size);

            vector<ByteArray> buffer2(buffer.size());
            decoder->Decode(buffer2.data(), buffer.size());

            for (uint32_t i = 0; i < buffer.size(); ++i) {
                EXPECT_EQ(buffer[i], buffer2[i]);
            }

            munmap(content, file_size);
            close(fd);
        }

        TEST(DeltaBinaryTest, Skip) {
            std::ifstream infile("testres/encoding/comment");
            string lineval;
            vector<string> strbuffer;
            vector<ByteArray> buffer;
            while (std::getline(infile, lineval)) {
                strbuffer.push_back(lineval);
                buffer.push_back(ByteArray(strbuffer.back()));
            }

            struct stat st;
            stat("testres/encoding/comment.binary", &st);
            auto file_size = st.st_size;
            auto fd = open("testres/encoding/comment.binary", O_RDONLY, 0);
            //Execute mmap
            void *mmappedData = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            assert(mmappedData != MAP_FAILED);
            auto content = (uint8_t *) mmappedData;

            auto decoder_raw = MakeDecoder(Type::BYTE_ARRAY, Encoding::DELTA_BYTE_ARRAY, nullptr);

            auto decoder = dynamic_cast<TypedDecoder<ByteArrayType> *>(decoder_raw.get());
            decoder->SetData(buffer.size(), content, file_size);

            auto counter = 0u;
            srand(time(NULL));

            vector<ByteArray> buffer2(buffer.size());

            while (counter < buffer.size()) {
                uint32_t skip = rand() % 10;
                uint32_t read = rand() % 20;

                decoder->Skip(skip);
                decoder->Decode(buffer2.data(), read);

                for (uint32_t i = 0; i < read; ++i) {
                    if(counter+skip+i < buffer.size()) {
                        EXPECT_EQ(buffer[counter + skip + i], buffer2[i]) << (counter + skip + i);
                    }
                }
                counter += skip + read;
            }
            munmap(content, file_size);
            close(fd);
        }

    }
}