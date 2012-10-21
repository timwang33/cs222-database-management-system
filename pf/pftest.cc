#include <iostream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;

// Check if a file exists

bool FileExists(string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

int PFTest_1(PF_Manager *pf) {
	// Functions Tested:
	// 1. CreateFile
	cout << "****In PF Test Case 1****" << endl;

	RC rc;
	string fileName = "test";

	// Create a file named "test"
	rc = pf->CreateFile(fileName.c_str());
	assert(rc == success);

	if (FileExists(fileName.c_str())) {
		cout << "File " << fileName << " has been created." << endl << endl;
		return 0;
	} else {
		cout << "Failed to create file!" << endl;
		return -1;
	}

	// Create "test" again, should fail
	rc = pf->CreateFile(fileName.c_str());
	assert(rc != success);

	return 0;
}

int PFTest_2(PF_Manager *pf) {
	// Functions Tested:
	// 1. OpenFile
	// 2. AppendPage
	// 3. GetNumberOfPages
	// 4. WritePage
	// 5. ReadPage
	// 6. CloseFile
	// 7. DestroyFile
	cout << "****In PF Test Case 2****" << endl;

	RC rc;
	string fileName = "test";

	// Open the file "test"
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);

	// Append the first page
	// Write ASCII characters from 32 to 125 (inclusive)
	void *data = malloc(PF_PAGE_SIZE);
	for (unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *) data + i) = i % 94 + 32;
	}
	rc = fileHandle.AppendPage(data);
	assert(rc == success);

	// Get the number of pages
	unsigned count = fileHandle.GetNumberOfPages();
	assert(count == (unsigned)1);

	// Update the first page
	// Write ASCII characters from 32 to 41 (inclusive)
	data = malloc(PF_PAGE_SIZE);
	for (unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *) data + i) = i % 94 + 32;
	}
	rc = fileHandle.WritePage(0, data);
	assert(rc == success);

	// Read the page
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(0, buffer);
	cout << (char*) buffer;
	assert(rc == success);

	// Check the integrity
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	rc = fileHandle.WritePage(0, data);
	assert(rc == success);
	// Close the file "test"
	rc = pf->CloseFile(fileHandle);
	assert(rc == success);

	free(data);
	free(buffer);

	if (FileExists(fileName.c_str())) {
		// DestroyFile
		rc = pf->DestroyFile(fileName.c_str());
		assert(rc == success);
	}
	if (!FileExists(fileName.c_str())) {
		cout << "File " << fileName << " has been destroyed." << endl;
		cout << "Test Case 2 Passed!" << endl << endl;
		return 0;
	} else {
		cout << "Failed to destroy file!" << endl;
		return -1;
	}
}

int PFTest_4(PF_Manager *pf) {
	cout << "****In PF Test Case 4****" << endl;
	string file = "file4.txt";

	RC rc;

	if (FileExists(file.c_str())) {
		rc = pf->DestroyFile(file.c_str());
		assert(rc == success);
	}

	PF_FileHandle fileHandle;
	PF_FileHandle fileHandle1;
	PF_FileHandle fileHandle2;

	rc = pf->CreateFile(file.c_str());
	assert(rc == success);
	rc = pf->OpenFile(file.c_str(), fileHandle);
	assert(rc == success);
	rc = pf->OpenFile(file.c_str(), fileHandle1);
	assert(rc == success);
	rc = pf->OpenFile(file.c_str(), fileHandle);
	assert(rc == success);
	rc = pf->CloseFile(fileHandle);
	assert(rc == success);
	rc = pf->CloseFile(fileHandle);
	assert(rc != success);
	rc = pf->CloseFile(fileHandle1);
	assert(rc == success);
	rc = pf->DestroyFile(file.c_str());
	assert(rc == success);
	return 0;
}
int PFTest_3(PF_Manager *pf) {

	cout << "****In PF Test Case 3****" << endl;
	string file = "file2.txt";

	RC rc;

	if (FileExists(file.c_str())) {
		rc = pf->DestroyFile(file.c_str());
		assert(rc == success);
	}

	PF_FileHandle fileHandle;
	rc = pf->CreateFile(file.c_str());
	assert(rc == success);
	rc = pf->OpenFile(file.c_str(), fileHandle);
	assert(rc == success);
	void *data = malloc(PF_PAGE_SIZE);
	void *buffer = malloc(PF_PAGE_SIZE);
	for (unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *) data + i) = i % 20 + 32;
	}
	rc = fileHandle.WritePage(0, data);
	assert(rc == success);
	rc = fileHandle.WritePage(1, data);
	assert(rc == success);
	rc = fileHandle.WritePage(2, data);
	assert(rc == success);
	rc = fileHandle.ReadPage(2, buffer);
	assert(rc == success);
	int character;
	for (unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		character = (int) *((char *) buffer + i);
		putchar(character);
	}
	rc = fileHandle.AppendPage(data);
	assert(rc == success);
	PF_FileHandle fileHandle1;
	rc = pf->OpenFile(file.c_str(), fileHandle1);
	assert(rc == success);
	rc = pf->CloseFile(fileHandle);
	assert(rc == success);
	rc = pf->CreateFile("me.txt");
	assert(rc == success);
	rc = pf->OpenFile("me.txt", fileHandle1);
	assert(rc != success);
	rc = pf->OpenFile("me.txt", fileHandle);
	assert(rc == success);
	rc = pf->CloseFile(fileHandle1);
	assert(rc == success);
	rc = pf->DestroyFile(file.c_str());
	assert(rc == success);
	rc = pf->OpenFile(file.c_str(), fileHandle);
	assert(rc != success);
	free(data);
	free(buffer);
	return 0;
}

/*int main() {
	PF_Manager *pf = PF_Manager::Instance();
	remove("test");

	PFTest_4(pf);
	//PFTest_2(pf);
	//PFTest_3(pf);

	return 0;
}*/
