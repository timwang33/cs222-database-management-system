#include "pf.h"

#include <sys/stat.h>

PF_Manager* PF_Manager::_pf_manager = 0;

PF_Manager* PF_Manager::Instance() {
	if (!_pf_manager)
		_pf_manager = new PF_Manager();

	return _pf_manager;
}

PF_Manager::PF_Manager() {

}

PF_Manager::~PF_Manager() {
}



RC PF_Manager::CreateFile(const char *fileName) {
	FILE *pfile;

	struct stat stFileInfo;

	if (stat(fileName, &stFileInfo) == 0)
			return RC_FAIL;
	else {
		pfile = fopen(fileName, "wb"); //create
		if (pfile!= NULL) {
			fclose(pfile);
			return RC_SUCCESS;
		}
	}
	return RC_FAIL;
}


RC PF_Manager::DestroyFile(const char* fileName) {
	return remove(fileName);

}

RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle) {


		FILE* fp = fopen(fileName, "rb+");

		if (fp != NULL) {
			if (fileHandle.HasHandle() == RC_FAIL) {
				fileHandle.nameOfFile = (char*)fileName;
				fileHandle.SetHandle(fp);
				return RC_SUCCESS;
			}
			else {
				if (fileHandle.nameOfFile == fileName) {
					fclose(fp);
					return RC_SUCCESS;
				}
			}
			fclose(fp);
			return RC_FAIL;
		}

		return RC_FAIL;


}

RC PF_Manager::CloseFile(PF_FileHandle &fileHandle) {
	FILE* fp = fileHandle.GetHandle();
if (fp == NULL) return RC_FAIL;
	RC rc = fclose(fp);

	if (rc != RC_SUCCESS)
		return RC_FAIL;
	fileHandle.ClearHandle();
	return rc;
}

PF_FileHandle::PF_FileHandle() {
	handle = NULL;
}

PF_FileHandle::~PF_FileHandle() {
	nameOfFile = NULL;
	if (handle!=NULL) handle =NULL;
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data) {

	if (pageNum >= GetNumberOfPages()) return RC_FAIL;

	long int offset = pageNum * PF_PAGE_SIZE;

	fseek(handle, offset, SEEK_SET);

	int size;

	size = fread(data, 1, PF_PAGE_SIZE, handle);

	if (size == PF_PAGE_SIZE) return RC_SUCCESS;

		return RC_FAIL;
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data) {

	PageNum totalNum = GetNumberOfPages();
	if (pageNum == totalNum) return AppendPage(data);
	else if (pageNum > totalNum) return RC_FAIL;

		long int offset = pageNum * PF_PAGE_SIZE;

		fseek(handle, offset, SEEK_SET);

		int size;

		size = fwrite(data, 1, PF_PAGE_SIZE, handle);

		if (size == PF_PAGE_SIZE) return RC_SUCCESS;

		return RC_FAIL;

}

RC PF_FileHandle::AppendPage(const void *data) {
	fseek(handle, 0, SEEK_END);

	int size;

	size = fwrite(data, 1, PF_PAGE_SIZE, handle);

	if (size == PF_PAGE_SIZE) return RC_SUCCESS;

	return RC_FAIL;
}

RC PF_FileHandle::ClearHandle() {
		handle = NULL;

		return RC_SUCCESS;
}
RC PF_FileHandle::SetHandle(FILE* fHandle) {
	if (fHandle != NULL) {
		handle = fHandle;
		return RC_SUCCESS;
	}
	return RC_FAIL;

}

RC PF_FileHandle::HasHandle() {
	if (handle != NULL)return RC_SUCCESS;
	return RC_FAIL;

}

FILE* PF_FileHandle::GetHandle() {
	return handle;
}

unsigned PF_FileHandle::GetNumberOfPages() {
	unsigned size;
	assert(HasHandle()== RC_SUCCESS);

	fseek(handle, 0, SEEK_END);
	size = ftell(handle) / PF_PAGE_SIZE;
	return size;
}
