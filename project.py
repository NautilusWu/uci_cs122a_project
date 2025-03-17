import sys

import functions


def main():

    cmd_line = sys.argv
    # print(cmd_line)

    if len(cmd_line) < 2:
        print(
            "Usage: python project.py <function name> [param1] [param2] …"
        )
        return

    func_name = cmd_line[1].lower()
    match func_name:
        case "import":
            folder_path = cmd_line[2]
            rtn_bool = functions.f_import(folder_path)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "insertviewer":
            params = cmd_line[2:]
            rtn_bool = functions.f_insertviewer(params)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "addgenre":
            params = cmd_line[2:]
            rtn_bool = functions.f_addgenre(params)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "deleteviewer":
            params = cmd_line[2:]
            rtn_bool = functions.f_deleteviewer(params)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "insertmovie":
            params = cmd_line[2:]
            rtn_bool = functions.f_insertmovie(params)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "insertsession":
            params = cmd_line[2:]
            rtn_bool = functions.f_insertsession(params)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "updaterelease":
            params = cmd_line[2:]
            rtn_bool = functions.f_updaterelease(params)
            info = "Success" if rtn_bool else "Fail"
            print(info)
        case "listreleases":
            params = cmd_line[2:]
            rtn_bool = functions.f_listrelease(params)
            # info = "Success" if rtn_bool else "Fail"
            # print(info)
        case "popularrelease":
            params = cmd_line[2:]
            rtn_bool = functions.f_popularrelease(params)
            # info = "Success" if rtn_bool else "Fail"
            # print(info)
        case "releasetitle":
            params = cmd_line[2:]
            functions.f_releasetitle(params)
        case "activeviewer":
            params = cmd_line[2:]
            rtn_bool = functions.f_activeviewer(params)
        case "videosviewed":
            print(params)
            # params = cmd_line[2:]
            # rtn_bool = functions.f_videosviewed(params)
        case _:
            print("Unknown command: " + func_name)
            print(
                "Usage: python project.py <function name> [param1] [param2] …"
            )
            return
    return


if __name__ == "__main__":
    main()
