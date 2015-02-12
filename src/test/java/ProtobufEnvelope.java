import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.ByteArrayInputStream;
import java.util.HashMap;

/**
 * ProtobufEnvelope - allows creating a protobuf message without the .proto file dynamically.
 *
 * @author Florian Leibert
 */
public class ProtobufEnvelope {
    private HashMap<String, Object> values = new HashMap();
    private DescriptorProtos.DescriptorProto.Builder desBuilder;
    private int i = 1;

    public ProtobufEnvelope() {
        desBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        i = 1;
    }

    public <T> void addField(String fieldName, T fieldValue, DescriptorProtos.FieldDescriptorProto.Type type) {
        DescriptorProtos.FieldDescriptorProto.Builder fd1Builder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(fieldName).setNumber(i++).setType(type);
        desBuilder.addField(fd1Builder.build());
        values.put(fieldName, fieldValue);
    }

    public Message constructMessage(String messageName) throws Descriptors.DescriptorValidationException {
        desBuilder.setName(messageName);
        DescriptorProtos.DescriptorProto dsc = desBuilder.build();

        DescriptorProtos.FileDescriptorProto fileDescP = DescriptorProtos.FileDescriptorProto.newBuilder()
                .addMessageType(dsc).build();

        Descriptors.FileDescriptor[] fileDescs = new Descriptors.FileDescriptor[0];
        Descriptors.FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, fileDescs);
        Descriptors.Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName(messageName);
        DynamicMessage.Builder dmBuilder =
                DynamicMessage.newBuilder(msgDescriptor);
        for (String name : values.keySet()) {
            dmBuilder.setField(msgDescriptor.findFieldByName(name), values.get(name));
        }
        return dmBuilder.build();
    }

    public void clear() {
        desBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        i = 1;
        values.clear();
    }

    public Descriptors.Descriptor getDescriptor() throws Descriptors.DescriptorValidationException {
        addField("id", 53453, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        addField("name", "fssd", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        addField("person0", "fssd", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        addField("person1", "fssd", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        addField("person2", "fssd", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        addField("person3", "fssd", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);

        desBuilder.setName("Person");
        DescriptorProtos.DescriptorProto dsc = desBuilder.build();

        DescriptorProtos.FileDescriptorProto fileDescP = DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(dsc).build();
        Descriptors.FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName("Person");
        return msgDescriptor;
    }

    public static void main(String[] args) throws Exception {
        ProtobufEnvelope pe = new ProtobufEnvelope();
        Descriptors.Descriptor msgDescriptor = pe.getDescriptor();

        byte[] bytes = MyMessage.Person.newBuilder()
                .setId(53453)
                .setName("fssd")
                .setPerson0("fssd")
                .setPerson1("fssd")
                .setPerson2("fssd")
                .setPerson3("fssd").build().toByteArray();
//        DynamicMessage.Builder dmBuilder = DynamicMessage.newBuilder(MyMessage.getDescriptor().findMessageTypeByName("Person"));
//        for (String name : pe.values.keySet()) {
//            dmBuilder.setField(msgDescriptor.findFieldByName(name), pe.values.get(name));
//        }
//        DynamicMessage build = dmBuilder.build();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        for (int a = 0; a < 10_000; a++) {
            MyMessage.Person person = MyMessage.Person.parseFrom(in);
            in.reset();
//            DynamicMessage.parseFrom(new ProtobufEnvelope().getOtherDescriptor(), data);
        }

        long l = System.currentTimeMillis();
        for (int a = 0; a < 20_000_000; a++) {
            MyMessage.Person person = MyMessage.Person.parseFrom(in);
            in.reset();
//            DynamicMessage.parseFrom(msgDescriptor, data);
        }
        System.out.println(System.currentTimeMillis() - l);
    }
}